version_check:
ifeq ($(VERSION), )
	@echo "Supply VERSION"
	@exit
endif

version:
	find . -name "*.spec" -print -exec perl -i -n -e 's/^Version: (\d+\.\d+\.\d+)/Version: $(VERSION)/; print;' {} \;

rpm_source: version_check
	cd .. && git archive --prefix=libfq-$(VERSION)/ $(VERSION) \
	-o ~/rpmbuild/SOURCES/libfq-$(VERSION).tar.gz

