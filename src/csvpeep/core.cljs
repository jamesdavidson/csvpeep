(ns csvpeep.core
 (:require-macros [cljs.core.async.macros :refer [go go-loop]])
 (:require
  [cljsjs.aws-sdk-js]
  [cljs.core.async :refer [put! chan <! >! timeout close! onto-chan]]
  [clojure.walk :refer [keywordize-keys]]
  [cljs.reader :refer [read-string]]
  [reagent.core :as r]))

(defonce state (r/atom {
 ::bucket "csvpeep-424204171541"
 ::region "ap-southeast-2"
}))

(def hostname->redirect_uri {
 "localhost" "http://localhost:3449/index.html"
 "www.csvpeep.com" "https://www.csvpeep.com/index.html"
})

(defn cognito-url []
 (-> (new goog.Uri "https://csvpeep.auth.ap-southeast-2.amazoncognito.com/login")
   (.setParameterValue "client_id" "3nsbvm4qsekojes57srd88brbf")
   (.setParameterValue "response_type" "token")
   (.setParameterValue "redirect_uri" (hostname->redirect_uri js/window.location.hostname))
  str))

(defn get-presigned-url [creds m]
 (let [ch (chan)
       client (new js/AWS.S3 (-> creds (assoc "signatureVersion" "v4") clj->js))]
;  (put! ch (.getSignedUrl client "getObject" (clj->js m)))
  (put! ch (str "https://s3.console.aws.amazon.com/s3/object/" (get m "Bucket") "/" (get m "Key") "?region=ap-southeast-1"))
  ch))

(defn invoke-lambda-function [creds args]
 (let [ch (chan)
       client (new js/AWS.Lambda (clj->js creds))]
  (.invoke client (clj->js args)
   #(do
     (if %1 (js/console.error %1)
            (put! ch (-> %2 (aget "Payload") js/JSON.parse js->clj)))
     (close! ch)))
  ch))

(defn list-objects [args]
 (let [ch (chan)
       client (new js/AWS.S3)]
  (.listObjects client (clj->js args)
   #(do
     (if %1 (js/console.error %1)
            (put! ch (-> %2 js->clj keywordize-keys :Contents)))
     (close! ch)))
  ch))

(defn upload [args]
 (let [ch (chan)
       client (new js/AWS.S3)
       stream (.upload client (clj->js args)
               #(do
                 (if %1
                  (js/console.error %1)
                  (put! ch (-> %2 js->clj keywordize-keys)))
                 (close! ch)))]
  (.on stream "httpUploadProgress" #(put! ch (-> %1 js->clj keywordize-keys)))
  ch))

(defn select-object-content [args]
 (let [ch (chan)
       client (new js/AWS.S3)]
  (.selectObjectContent client (clj->js args)
   #(do
     (if %1 (js/console.error %1)
            (put! ch (->> %2 js->clj keywordize-keys :Payload
                          (map :Records)
                          (map :Payload)
                          (apply str))))
     (close! ch)))
  ch))

(defn get-object-columns [args]
 (let [ch (chan)
       params (-> args (assoc :Expression "SELECT * FROM s3object LIMIT 1")
                       (assoc :ExpressionType "SQL")
                       (update :InputSerialization assoc :CSV {:FileHeaderInfo "NONE"})
                       (assoc :OutputSerialization {:CSV {:QuoteFields "ALWAYS"}}))]
  (go
   (when-let [result (<! (select-object-content params))]
    (let [names (map read-string (-> result
                                    clojure.string/split-lines first
                                    (.replace "\r" "") ;; workaround for S3's buggy quoting of last value in any given row
                                    (clojure.string/split ",")))
          columns (apply array-map (interleave names (repeat {})))]
     (put! ch columns)))
   (close! ch))
  ch))

(defn refresh-list-of-objects! []
 (let [{::keys [bucket]} @state]
  (go
   (when-let [objects (<! (list-objects {:Bucket bucket}))]
    (swap! state update ::objects merge (into {} (map (juxt :Key identity) objects)))))))

(def probably-csv?
 #(or (.endsWith % ".csv.bz2")
      (.endsWith % ".csv.gz")
      (.endsWith % ".csv")))

(defn list-of-objects []
 (let [{::keys [bucket objects region query records key files]} @state]
  (when (nil? objects) (refresh-list-of-objects!))
  [:div
   [:h1 "CSV Peep"]
   [:br]
   [:p "Which CSV file would you like to analyze?"]
   [:br]
   (into [:div]
    (for [[k obj] objects]
     (let [{::keys [file progress]} (meta obj)]
      [:p
       [:button
        {:class (if file "btn btn-secondary" "btn btn-primary")
         :style {:width "100%"}
         :on-click (fn []
                    (if file
                     ;; upload to S3
                     (let [stream (upload {:Bucket bucket :Key k :Body file})]
                      (go-loop [evt (<! stream)]
                       (let [{:keys [loaded total ETag]} evt]
                        (if ETag
                         (swap! state update ::objects assoc k evt)
                         (do (swap! state update-in [::objects k]
                                    vary-meta assoc ::progress (int (* 100 (/ loaded total))))
                             (recur (<! stream)))))))
                     ;; get list of columns then switch to table view
                     (let [params {:Bucket bucket,
                                   :Key k,
                                   :InputSerialization {:CompressionType (cond (.endsWith k ".gz") "GZIP"
                                                                               (.endsWith k ".bz2") "BZIP2"
                                                                               :default "NONE")}}]
                      (go
                       (when-let [cols (<! (get-object-columns params))]
                        (swap! state merge {::key k ::query {::limit 10 ::columns cols}}))))))} k]
       (when progress
        [:div.progress
         [:div.progress-bar {:role "progressbar" :style {:width (str progress "%")}
                             :aria-valuenow (str progress) :aria-valuemin "0" :aria-valuemax "100"} (str progress "%")]])
       [:br]])))
   [:br]
   [:p "Or would you like to upload your own?"]
   [:input {:type "file" :multiple true
            :on-change #(let [file-list (-> % .-target .-files)
                              files (into {}
                                     (for [i (range (aget file-list "length"))]
                                      (let [f (aget file-list i)
                                            k (aget f "name")]
                                       [k (with-meta {:Key k} {::file f})])))]
                         (swap! state update ::objects merge files))
            :style {:min-height "200px" :width "100%"
                    :display (if (nil? files) "inline-block" "none")
                    :background-color "#eee" :border "1px dashed black"}}]
   [:br]]))

(defn column-label [k]
 (let [{::keys [menu]} (get-in @state [::query ::columns k])]
  [:div.container
   [:div {:class (if menu "dropdown show" "dropdown")}
    [:a {:href "#" :on-click #(swap! state update-in [::query ::columns k ::menu] not)
         :style {:color "black"}} k]
    [:div {:class (if menu "dropdown-menu show" "dropdown-menu")}
     [:a.dropdown-item {:href "#" :on-click #(swap! state update-in [::query ::columns] dissoc k)} "Hide"]]]]))

(defn ->expression [{::keys [columns limit]}]
 (str "SELECT " (if (empty? columns) "*" (clojure.string/join ", " (map #(str "\"" % "\"") (keys columns)))) " FROM s3object s"
      " LIMIT " (if (nil? limit) 1 (int limit))))

(add-watch state "select-object-contents"
                 (fn [_ _ o n]
                  (when (and
                         (not (nil? (::key n)))
                         (or (not (= (-> o ::query ::columns keys)
                                     (-> n ::query ::columns keys)))
                             (not (= (-> o ::query ::limit)
                                     (-> n ::query ::limit)))
                             (not (= (::key o) (::key n)))))
                   (go
                    (let [params {:Bucket (::bucket n),
                                  :Key (::key n),
                                  :Expression (->expression (::query n)),
                                  :ExpressionType "SQL",
                                  :InputSerialization {:CSV {:FileHeaderInfo "USE"}
                                                       :CompressionType (cond (.endsWith (::key n) ".gz") "GZIP"
                                                                              (.endsWith (::key n) ".bz2") "BZIP2"
                                                                              :default "NONE")},
                                  :OutputSerialization {:JSON {}}}]
                     (when-let [records (->> (<! (select-object-content params))
                                          clojure.string/split-lines
                                          (map #(try (js/JSON.parse %) (catch js/Error _ nil)))
                                          (remove nil?)
                                          (map js->clj))]
                      (swap! state assoc ::records records)))))))

(def inc-x2
 #(cond (= 0 %) 1
        (nil? %) 1
        :default (* 2 %)))

(defn run-query []
 (let [{::keys [bucket objects region query records key show-sql]} @state]
  (when-not (nil? key)
   [:div
    (when show-sql
     [:pre
       {:style {:margin "auto" :width "80%" :word-wrap "break-word"}}
       (->expression query)])
    (let [ks (-> @state ::query ::columns keys)]
     [:table {:class "table table-striped"}
      [:thead (into [:tr] (for [k ks] [:th (column-label k)]))]
      (into [:tbody] (for [x records] (into [:tr] (for [k ks] (vector :td (get x k))))))])
    [:p {:style {:text-align "center"}}
     (if (nil? records)
      "loading..."
      [:a {:href "#" :on-click #(swap! state update-in [::query ::limit] inc-x2)} "load more"])]])))

(defn home-page []
 (let [{::keys [key show-sql]} @state]
  [:div.container
   (when-not (nil? key)
    [:div.float-left.btn-group-vertical {:role "group"}
     [:button.btn.btn-secondary
      {:on-click (fn [] (swap! state #(select-keys % [::bucket ::region ::objects])))} "back"]
     [:button.btn.btn-light
      {:on-click #(swap! state update ::show-sql not)} (if show-sql "hide sql" "show sql")]])
    [:div {:style {:margin "auto" :max-width "1024px"}}
     (if (nil? key) [list-of-objects] [run-query])]]))

(defn mount-root []
 (r/render [home-page] (.getElementById js/document "app")))

(defn init! []
 (do
  (set! js/AWS.config.region "ap-southeast-2")
  (set! js/AWS.config.credentials
   (new js/AWS.CognitoIdentityCredentials
    (if-let [id_token (.get (new js/URLSearchParams (.substring js/window.location.hash 1)) "id_token")]
     #js{:IdentityPoolId "ap-southeast-2:f0c61e74-d62f-4347-851f-0f4c714fac78",
         :Logins #js{"cognito-idp.ap-southeast-2.amazonaws.com/ap-southeast-2_yQBJcf31A" id_token}}
     #js{:IdentityPoolId "ap-southeast-2:f0c61e74-d62f-4347-851f-0f4c714fac78"})))
  (.refresh js/AWS.config.credentials)
  (.pushState js/history nil nil "index.html")
  (mount-root)))
