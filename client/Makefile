#!/usr/bin/make -f

all:
	python setup.py build

install:
	mkdir -p $(DESTDIR)
	python setup.py install --prefix=/usr --exec-prefix=/usr --root=$(DESTDIR)

clean:
	python setup.py clean --all
	rm -rf lib/*.egg-info
