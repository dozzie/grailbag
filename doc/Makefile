#!/usr/bin/make -f

.PHONY: all doc man html clean distclean

all doc: html

html:
	sphinx-build -b $@ -d doctrees -- . $@

man:
	sphinx-build -b $@ -d doctrees -- . $@
	bin/postprocess-manpages man/*.[1-8]

clean:
	rm -rf man html

distclean: clean
	rm -rf doctrees
