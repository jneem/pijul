# This was a temporary fork of pijul!

The real repository is [here](https://nest.pijul.com/pijul/pijul), but I was running into some corruption bugs.

# Pijul

Pijul is a version control system based on *patches*, that can mimic
the behaviour and workflows of both [Git](https://git-scm.org) and
[Darcs](https://darcs.net), but contrarily to those systems, Pijul is
based on a **mathematically sound theory of patches**.


Pijul was started out of frustration that no version control system
was at the same time fast and sound:

- Git has non-associative merges, which might lead to security problems. Concretely, this means that the commits you merge might not be the same as the ones you review and test. [More details here](https://nest.pijul.com/help/patches.html).

- Handling of conflicts: Pijul has an explicit internal representation of conflicts, a rock-solid theory of how they behave, and super-fast data structures to handle them.

- Speed! The complexity of Pijul is low in all cases, whereas previous attempts to build a mathematically sound distributed version control system had huge worst-case complexities. The use of [Rust](//www.rust-lang.org) additionally yields a blazingly fast implementation.


## License

The license is GPL2, or any later version at your convenience. This was changed from the time when Pijul was still a prototype, and had another license.

## Contributing

We welcome contributions, even if you understand nothing of patch theory.
Currently, the main areas where Pijul needs improvements are:

- Portable handling of SSH keys (Windows and Linux).
- Error messages. There are very few useful messages at the moment.
- HTTP Redirects and errors.

The first step towards contributing is to *clone the repositories*. Pijul depends on a number of packages maintained by the same team, the two largest ones being [Sanakirja](/pijul_org/sanakirja) and [Thrussh](/pijul_org/thrussh).
Here is how to build and install the pijul repositories:

```
$ pijul clone https://nest.pijul.com/pijul_org/pijul
$ cd pijul
$ cargo install
```

By contributing, you agree to make all your contributions GPL2+.

Moreover, the main platform for contributing is [the Nest](//nest.pijul.com/pijul_org/pijul), which is still at an experimental stage. Therefore, even though we do our best to avoid it, our repository might be reset, causing the patches of all contributors to be merged.
