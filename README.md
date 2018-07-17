# datacat [![Build Status](https://travis-ci.org/joakim-brannstrom/datacat.svg?branch=master)](https://travis-ci.org/joakim-brannstrom/datacat)

**datacat** is a lightweight Datalog engine intended to be embedded in other D programs.

# Getting Started

datacat depends on the following software packages:

 * [D compiler](https://dlang.org/download.html) (dmd 2.079+, ldc 1.8.0+)

Download the D compiler of your choice, extract it and add to your PATH shell
variable.
```sh
# example with an extracted DMD
export PATH=/path/to/dmd/linux/bin64/:$PATH
```

Once the dependencies are installed it is time to download the source code to install datacat.
```sh
git clone https://github.com/joakim-brannstrom/datacat.git
cd datacat
dub build -b release
```

Done! Have fun.
Don't be shy to report any issue that you find.

# Credit

All credit goes to Frank McSherry <fmcsherry@me.com> for the excellent blog post and implementation (this port). I highly recommend to read [Frank's blog](https://github.com/frankmcsherry/blog/blob/master/posts/2018-05-19.md).
