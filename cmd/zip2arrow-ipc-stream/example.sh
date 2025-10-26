#!/bin/sh

izip="./sample.d/input.zip"

geninput(){
	echo creating input files...

	mkdir -p ./sample.d

	echo helo > ./sample.d/f1.txt
	echo world > ./sample.d/f2.txt

	ls ./sample.d/f?.txt |
		zip \
			-@ \
			-T \
			-v \
			-o \
			"${izip}"
}

test -f "${izip}" || geninput

ENV_ZIP_FILE_NAME="${izip}" ./zip2arrow-ipc-stream |
	arrow-cat

which rs-ipc-stream2df        | fgrep -q rs-ipc-stream2df        || exit 0
which rs-arrow-ipc-stream-cat | fgrep -q rs-arrow-ipc-stream-cat || exit 0

ENV_ZIP_FILE_NAME="${izip}" ./zip2arrow-ipc-stream |
	rs-ipc-stream2df \
		--tabname 'zip_table' \
		--sql '
			SELECT
				name,
				compressed_size,
				uncompressed_size,
				crc32,
				method,
				zip_filename,
				TO_UNIXTIME(modified) AS unixtime
			FROM zip_table
			ORDER BY uncompressed_size DESC
		' |
	rs-arrow-ipc-stream-cat
