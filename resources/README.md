Polytely Analytics Resources
==================
In this section, we will publish our code and explorations while implementing some interesting algorithms and testing their scalability (perform incrementally and perform parallel), robustness to noise (real data have significant noise even if there is a good theory to explain its generating process) and the structure of the data that can break them.

We are in the process of building an open source decision support system that leverages the availability of public domain data, so as to provide insights on the dynamics that govern a national power market. In particular, we want to provide the functionality to collect, transform and analyze power market data, as well as to visualize the monetary flows between the relevant market actors. The task of data analysis will be based on recursive partitioning and regression trees.

The *Greek power market* subdirectory in the *data* folder contains code (util.py) for:

1.  Downloading all necessary (excel) files: plant availability data from the Hellenic Independent Power Transmission Operator S.A. (ADMIE) and day-ahead scheduling (DAS) results data from the Hellenic Electricity Market Operator S.A. (LAGIE);
2. Extracting the relevant data from the downloaded files and storing them in a running instance of MongoDB.