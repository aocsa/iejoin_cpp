CXXFLAGS = -Wall -Wextra -std=c++20 -O3

# Boost library settings
BOOST_INCLUDE = $(CONDA_PREFIX)/include
BOOST_LIB = $(CONDA_PREFIX)/lib
BOOST_LIBS = -lboost_system -lboost_filesystem

# Source and object files
SRC = main.cpp
OBJ = $(SRC:.cpp=.o)

# Target executable
TARGET = app

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CXX) $(CXXFLAGS) -L$(BOOST_LIB) $^ -o $@ $(BOOST_LIBS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -I$(BOOST_INCLUDE) -c $< -o $@

clean:
	rm -f $(OBJ) $(TARGET)