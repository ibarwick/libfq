libfq_subdir = doc
libfq_top_builddir = ..

ifndef JADE
JADE = $(missing) jade
endif

srcdir = .

SGMLINCLUDE = -D . -D ${srcdir}

SPFLAGS += -wall -wno-unused-param -wno-empty -wfully-tagged

JADE.html.call = $(JADE) $(JADEFLAGS) $(SPFLAGS) $(SGMLINCLUDE) $(CATALOG) -t sgml -i output-html
MKDIR_P = mkdir -p
ALLSGML := $(wildcard $(srcdir)/*.sgml)
# to build bookindex
ALMOSTALLSGML := $(filter-out %bookindex.sgml,$(ALLSGML))
GENERATED_SGML = version.sgml bookindex.sgml

LIBFQ_VERSION_STRING=$(shell awk '/^\#define LIBFQ_VERSION_STRING / { print $3; }' $(libfq_top_builddir)/include/libfq-version.h | cut -d '"' -f 2)

all: html

html: html-stamp

html-stamp: libfq.sgml $(ALLSGML) $(GENERATED_SGML) stylesheet.dsl website-docs.css
	@$(MKDIR_P) html
	$(JADE.html.call) -d stylesheet.dsl -i include-index $<
	cp $(srcdir)/stylesheet.css $(srcdir)/website-docs.css html/
	touch $@

libfq.html: libfq.sgml $(ALLSGML) $(GENERATED_SGML) stylesheet.dsl website-docs.css
	sed '/html-index-filename/a\
(define nochunks  #t)' <stylesheet.dsl >nochunks.dsl
	$(JADE.html.call) -d nochunks.dsl -i include-index $< >libfq.html

version.sgml: $(libfq_top_builddir)/include/libfq-version.h
		{ \
	  echo "<!ENTITY libfqversion \"$(LIBFQ_VERSION_STRING)\">"; \
	} > $@



HTML.index: libfq.sgml $(ALMOSTALLSGML) stylesheet.dsl
	@$(MKDIR_P) html
	$(JADE.html.call) -d stylesheet.dsl -V html-index $<


website-docs.css:
	@$(MKDIR_P) html
	curl https://www.postgresql.org/media/css/docs.css > ${srcdir}/website-docs.css

bookindex.sgml: HTML.index
ifdef COLLATEINDEX
	LC_ALL=C $(PERL) $(COLLATEINDEX) -f -g -i 'bookindex' -o $@ $<
else
	collateindex.pl -o $@ $<
endif

clean:
	rm -f html-stamp
	rm -f HTML.index $(GENERATED_SGML)

maintainer-clean:
	rm -rf html
	rm -rf Makefile

zip: html
	cp -r html libfq-docs-$(LIBFQ_VERSION)
	zip -r libfq-docs-$(LIBFQ_VERSION).zip libfq-docs-$(LIBFQ_VERSION)
	rm -rf libfq-docs-$(LIBFQ_VERSION)

install: html
	@$(MKDIR_P) $(DESTDIR)$(docdir)/$(docmoduledir)/libfq
	@$(INSTALL_DATA) $(wildcard html/*.html) $(wildcard html/*.css) $(DESTDIR)$(docdir)/$(docmoduledir)/libfq
	@echo Installed docs to $(DESTDIR)$(docdir)/$(docmoduledir)/libfq

.PHONY: html all
