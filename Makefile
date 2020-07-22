BUCKET = www.csvpeep.com
PAGES = index

deploy:
	/usr/local/bin/lein package
	$(eval CACHE_BUSTER := $(shell md5sum < public/js/app.js | cut -b 1-10))
	cp -v public/js/app.js public/js/app.$(CACHE_BUSTER).js
	aws s3 cp --profile cicd public/js/app.$(CACHE_BUSTER).js s3://${BUCKET}/js/
	sed -i -e s/app.js/app.$(CACHE_BUSTER).js/ public/index.html
	for x in $(PAGES) ; do aws s3 cp --profile cicd public/index.html s3://${BUCKET}/$${x}.html --cache-control max-age=60 ; done

.PHONY = deploy
