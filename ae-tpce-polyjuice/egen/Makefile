CXX=g++
INCFLAGS=-Iprj
CXXFLAGS=-g \
		 -Wall \
		 -O2 \
		 -funroll-loops

EGENFLAGS=

src=$(wildcard src/*.cpp)
utils_src=$(wildcard Utilities/src/*.cpp)
input_files_src=$(wildcard InputFiles/src/*.cpp)

obj=$(patsubst %.cpp, %.o, $(src))
utils_obj=$(patsubst %.cpp, %.o, $(utils_src))
input_files_obj=$(patsubst %.cpp, %.o, $(input_files_src))

%.o: %.cpp
	$(CXX) $(INCFLAGS) $(CXXFLAGS) $(EGENFLAGS) -c $< -o $@

.PHONY: egenlib
egenlib/egenlib.a: $(obj) $(utils_obj) $(input_files_obj)
	@mkdir egenlib
	@ar r egenlib/egenlib.a $(obj) $(utils_obj) $(input_files_obj)

.PHONY: clean
clean:
	@rm -f src/*.o
	@rm -f Utilities/src/*.o
	@rm -f InputFiles/src/*.o
	@rm -rf egenlib
