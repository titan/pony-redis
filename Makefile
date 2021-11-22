include .config
NAME-LINK = $(subst _,-,$(NAME))

ESCAPED-BUILDDIR = $(shell echo '$(BUILDDIR)' | sed 's%/%\\/%g')
TARGET = $(BUILDDIR)/$(NAME-LINK)
BUILDSCRIPTS = corral.json lock.json
DSTSCRIPTS = $(BUILDSCRIPTS:%=$(BUILDDIR)/%)
SRCDIR = $(NAME)
SRCS = $(wildcard $(SRCDIR)/*.pony)
DSTSRCS = $(SRCS:%=$(BUILDDIR)/%)
TESTDIR = $(BUILDDIR)/test
TESTBIN = $(TESTDIR)/$(NAME)

all: test

test: $(TESTBIN)

$(TESTBIN): $(DSTSCRIPTS) $(DSTSRCS)
	cd $(BUILDDIR); corral fetch; corral run -- ponyc -o $(TESTDIR) $(NAME); cd -

$(DSTSCRIPTS): $(BUILDDIR)/%: % | prebuild
	cp $< $@

$(DSTSRCS): $(BUILDDIR)/%: % .config | prebuild
	cp $< $@

prebuild:
ifeq "$(wildcard $(BUILDDIR))" ""
	@mkdir -p $(BUILDDIR)/$(SRCDIR)
endif

clean:
	rm -rf $(BUILDDIR)

.PHONY: all clean install prebuild test
