Many of the projects in this solution require the Boost Test Library.
A free binary installer for Visual C++ can be downloaded from BoostPro
(see http://www.boostpro.com/download/).

Projects in this solution use $(BOOST_ROOT) to find the necessary header
and library files. This environment variable is NOT set automatically as
part of the above mentioned binary installation process. Therefore, after
installing the boost libraries, define the environment variable BOOST_ROOT
as appropriate (e.g. C:\Program Files (x86)\boost\boost_1_47).
