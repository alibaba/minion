ARCH_FILES="peer setup.py minion.py"

sources:
	git archive HEAD "${ARCH_FILES}" -o minion.tar.gz

