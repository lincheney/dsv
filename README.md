# dsv

A collection of coreutils-ish tools that are CSV-aware, TSV-aware etc.
Think `grep`, `cut`, `tac` etc. but CSV-aware.

`dsv` is *not* built to be fast, but *intuitive* to use
for people who spend a lot of time in the terminal and are used to using shell commands.
If you need something fast, or have big data to process,
or are *not* familiar with coreutils tools, `dsv` may not be for you.

> I used to use [xsv](https://github.com/BurntSushi/xsv) and sometimes [csvkit](https://csvkit.readthedocs.io/)
> but I could never remember the different command names and different flags,
> I use the shell a lot and I just wanted something with a similar interface.
> [miller](https://github.com/johnkerl/miller) is the closest I've found, but eventually I decided to write my own.

## example

```bash
<file.csv dsv grep -C5 -w REGEX
```
does the basically same thing that `grep -C5 -w REGEX` except that
it always prints the header and `REGEX` matches will not span multiple columns.

## installation

ermm....

If you have `python3`, the quickest way is to just run `dsv` (or symlink it into your `$PATH` or something).

Also find shell completion scripts in [completions/](completions/dsv.zsh).

### others

* If you have `pypy3`, you can also run `dsv.pypy` (which is actually the same code) and this will be faster than cypython
    * for small inputs `pypy3` actually runs *slower* than `cpython` due to the JIT startup cost
* If you want even more performance, build the *rust* version (`cargo build --release`). You should get a `./target/release/dsv`
    * you can also download automated builds from: https://github.com/lincheney/dsv/releases/tag/nightly
    * note that there are differences between the rust and python versions
    * to run the python-based commands, you will still need `python3`
        * the other commands should work fine without python however

## commands / usage

Note:
* many commands have an additional `-k` flag to restrict their effects to certain columns,
    e.g. `dsv grep -k COLUMN ...` (why `-k`? because that's what `sort` uses)
* most commands take only input from stdin (i.e. no filename argument)
---

* `!`: pipe multiple commands together
    * e.g. `dsv ! grep something ! cut -f column ! head -n10 ! tojson`
* `cat`: like coreutils
* `cut`: like coreutils
* `flip`: prints each column on a separate line
* `fromhtml`: convert from html table
* `fromjson`: convert from json
* `frommarkdown`: convert from markdown table
* `grep`: like coreutils (also a bit like https://github.com/BurntSushi/ripgrep)
* `head`: like coreutils
* `join`: like coreutils
* `page`: view the file in a pager (less)
* `paste`: like coreutils
* `pipe`: pipe rows through a processs
    * e.g. `dsv pipe -- tr [:lower:] [:upper:]`
* `pretty`: pretty prints the file
* `py`: run python on each row
* `py-filter`: filter rows using python
* `py-groupby`: aggregate rows using python
* `replace`: replace text
    * similar to `rg --replace ...` (see https://github.com/BurntSushi/ripgrep)
* `reshape-long`: reshape to long format
* `reshape-wide`: reshape to wide format
* `set-header`: sets the header labels
* `sort`: like coreutils
* `sqlite`: use sql on the data
* `summary`: produce automatic summaries of the data, kind of like `summary()` in R
* `tac`: like coreutils
* `tail`: like coreutils
* `tocsv`: convert to csv
* `tojson`: convert to json
* `tomarkdown`: convert to markdown table
* `totsv`: convert to tsv
* `uniq`: like `sort | uniq ...`
* `xargs`: like `xargs` and GNU `parallel`

## rust vs python

> Why is there a rust and a python version? Because I wrote the python code first, then did the rust kinda for fun.

Differences:
* different regex engine/syntax: https://docs.python.org/3/library/re.html vs https://docs.rs/regex/latest/regex/#syntax
* different html engine: https://docs.python.org/3/library/html.html vs https://docs.rs/quick-xml/latest/quick_xml/
* rust pipeline `!` command is actually faster
    * the pipeline command allows you to chain multiple commands together `dsv ! CMD ARG ! CMD ARG ...`.
        Theoretically this is faster than `dsv CMD ARG | dsv CMD ARG ...` because it avoids having to re-parse the contents,
        but in practice with python, it is actually slower because python is single threaded
        whereas real shell pipes effectively allow multiprocessing.
        Rust does not have this problem.
* rust may be slower than `pypy3` for with heavy python based command usage
    * this is because it uses `python3`

## more examples

## other projects

* if you really need high performance or have some seriously huge data, consider https://github.com/dathere/qsv instead
* I used this for a long time: https://github.com/johnkerl/miller
* `gnuplot` is great for graphs on the terminal, especially the "braille" mode: http://www.gnuplot.info/
