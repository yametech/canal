
ifeq ($(GOOS),windows)
BINARY_EXT_LOCAL:=.exe
GOLANGCI_LINT:=golangci-lint.exe
export ARCHIVE_EXT = .zip
else
BINARY_EXT_LOCAL:=
GOLANGCI_LINT:=golangci-lint
export ARCHIVE_EXT = .tar.gz
endif


.PHONY: lint
lint:
	# Due to https://github.com/golangci/golangci-lint/issues/580, we need to add --fix for windows
	$(GOLANGCI_LINT) run --fix

.PHONY: test
test:
	go test



