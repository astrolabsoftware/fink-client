# Testing with real data

An alternative to generating fake streams (see the [tutorial](README.md)), is
to connect to the Fink test streams. We daily send replayed alerts (at a random rate for the moment), and you can access it. For this, you would use your Fink credentials given to you for accessing the livestream service, except that the topic names start with `ftest_` instead of `fink_`. For a list of available topics see [https://fink-broker.readthedocs.io/en/latest/topics/](https://fink-broker.readthedocs.io/en/latest/topics/).
