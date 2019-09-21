**IMDB has stopped updating the files that this script processes. You
can still download a frozen set as of late late 2017 at
ftp://ftp.fu-berlin.de/pub/misc/movies/database/frozendata/ .**

IMDB -> JSON
============

[IMDB][] provides an up-to-date [dump of their database][dump] which is
a very interesting source of data. However the format it is provided in
is outright rubbish. Each file has a different free text format and
polluted by documentation. *imdb2json* seeks to remedy this by parsing
the files and outputting fairly sane JSON.

*imdb2json* requires python 3.5 and I don't have any plans to make this
work with older versions of python. Also, this is a work in progress and
not every file and/or every piece of data is currently parsed. Feel free
to send patches/pull requests.

Usage
=====

You need to download the `.list.gz` files from the mirrors provided on
[IMDB website][dump] onto your machine first. Say you pick the Germany
mirror, below is an easy way to get all the files:

    BASE='ftp://ftp.fu-berlin.de/pub/misc/movies/database/frozendata/'
    curl -sl "$BASE" | grep '\.list\.gz$' |
      parallel -j2 curl -s -o {} "$BASE"{}

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

If you have a multicore machine, you should be able to get a faster
processing by running multiple copies of *imdb2json*:

    python imdb2json.py list title |
      parallel python imdb2json.py convert title {}.list.gz '>' {}.json

`list` command outputs the supported IMDB files one per line and GNU
parallel runs as many instances of *imdb2json* as there are CPU cores.
After the above is complete, you can merge all the JSON files into one:

    python imdb2json.py merge *.json > all.json

Fun stuff
=========

For starters, you can pretty print the output with something like the
awesome [jq][]:

    python imdb2json.py convert title ratings.list.gz | jq .

Now let's do something semi-useful. Let's find out the top 20
shows/movies with 100K+ votes:

    python imdb2json.py convert title ratings.list.gz | jq -r '
      [.] | map(select(.rating.votes > 100000)) |
      .[] | [.id, .rating.rank] | @tsv
    ' | sort -t$'\t' -k2 -rn | head -20

Movie by Soundtrack
===================

Another way to find movies to watch is to look up titles with
soundtracks from your favorite artists. Here's an example of finding
titles with >1000 ratings with soundtracks performed by Depeche Mode:

    grep -i 'depeche mode' all.json |
      jq -r 'select(.rating.votes > 1000) | [.id, .rating.rank] | @tsv' |
      sort -t$'\t' -k2 -rn

[IMDB]: http://www.imdb.com/
[dump]: http://www.imdb.com/interfaces
[jq]: https://stedolan.github.io/jq/
