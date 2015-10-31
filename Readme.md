IMDB -> JSON
============

[IMDB][] provides an up-to-date [dump of their database][dump] which is
a very interesting source of data. However the format it is provided in
is outright rubbish. Each file has a different free text format and
polluted by documentation. *imdb2json* seeks to remedy this by parsing
the files and outputting fairly sane JSON.

*imdb2json* requires a recent python 3 and I don't have any plans to
make this work with older versions of python. Also, this is a work in
progress and not every file and/or every piece of data is currently
parsed. Feel free to send patches/pull requests.

Usage
=====

You need to download the `.list.gz` files from the mirrors provided on
[IMDB website][dump] onto your machine first. Say you pick the Germany
mirror, below is an easy way to get all the files:

    BASE='ftp://ftp.fu-berlin.de/pub/misc/movies/database/'
    curl -sl "$BASE" | grep '\.list\.gz$' |
      parallel --gnu -j2 curl -s -o {} "$BASE"{}

*imdb2json* can read both gzipped and plain text files. It detects the
type of file based on its content so file names don't matter.
*imdb2json* categorizes the files into two kinds: names and titles.
Names refers to people — e.g. actors, directors, stunt people. Titles
refers to movies, TV series, episodes, etc. When running *imdb2json*,
you pass the kind of file and it will only process the files that match
that kind. Here's an example:

    python imdb2json.py convert title ratings.list.gz actors.list.gz

`actors.list.gz` is ignored because it's not the `title` kind. The above
will output one JSON object per line, containing all the info about a
single title. If more files are given, all the info about the same title
across all files are merged into a single JSON. To get all the info
about all titles, run:

    python imdb2json.py convert title *.list.gz

The above will take some time to run. Even with one file, *imdb2json*
still needs to sort the file first and so there will be a delay till the
first line is output.

It's easy to tweak the output to make it one big JSON list so it can be
loaded all at once (probably bad idea) or be pretty printed using other
programs like `json_reformat` from [yajl][] library:

    (
      echo '['
      python imdb2json.py convert title taglines.list.gz |
        sed '2,$s/^/,/'
      echo ']'
    ) | json_reformat | less

You can also convert each file separately and then merge them:

    python imdb2json.py merge actors.json directors.json

[IMDB]: http://www.imdb.com/
[dump]: http://www.imdb.com/interfaces
[yajl]: http://lloyd.github.com/yajl/
