# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.31

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /opt/homebrew/bin/cmake

# The command to remove a file.
RM = /opt/homebrew/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /Users/hassanamad/Projects/DataFrame

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/hassanamad/Projects/DataFrame/build

# Utility rule file for ExperimentalConfigure.

# Include any custom commands dependencies for this target.
include DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/compiler_depend.make

# Include the progress variables for this target.
include DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/progress.make

DataStructures_build/tests/CMakeFiles/ExperimentalConfigure:
	cd /Users/hassanamad/Projects/DataFrame/build/DataStructures_build/tests && /opt/homebrew/bin/ctest -D ExperimentalConfigure

DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/codegen:
.PHONY : DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/codegen

ExperimentalConfigure: DataStructures_build/tests/CMakeFiles/ExperimentalConfigure
ExperimentalConfigure: DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/build.make
.PHONY : ExperimentalConfigure

# Rule to build all files generated by this target.
DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/build: ExperimentalConfigure
.PHONY : DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/build

DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/clean:
	cd /Users/hassanamad/Projects/DataFrame/build/DataStructures_build/tests && $(CMAKE_COMMAND) -P CMakeFiles/ExperimentalConfigure.dir/cmake_clean.cmake
.PHONY : DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/clean

DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/depend:
	cd /Users/hassanamad/Projects/DataFrame/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/hassanamad/Projects/DataFrame /Users/hassanamad/Projects/DataStructures/tests /Users/hassanamad/Projects/DataFrame/build /Users/hassanamad/Projects/DataFrame/build/DataStructures_build/tests /Users/hassanamad/Projects/DataFrame/build/DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/DependInfo.cmake "--color=$(COLOR)"
.PHONY : DataStructures_build/tests/CMakeFiles/ExperimentalConfigure.dir/depend

