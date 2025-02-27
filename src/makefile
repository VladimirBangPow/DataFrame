CC = gcc
CFLAGS = -Wall -Wextra -O2

SRC = \
	main.c \
	DataFrame/core/dataframe_core.c DataFrame/core/dataframe_core_test.c \
	DataFrame/query/dataframe_query.c DataFrame/query/dataframe_query_test.c \
	DataFrame/print/dataframe_print.c DataFrame/print/dataframe_print_test.c \
	DataFrame/io/dataframe_io.c DataFrame/io/dataframe_io_test.c \
	DataFrame/plot/dataframe_plot.c DataFrame/plot/dataframe_plot_test.c \
	DataFrame/date/dataframe_date.c DataFrame/date/dataframe_date_test.c \
	DataFrame/indexing/dataframe_indexing.c DataFrame/indexing/dataframe_indexing_test.c \
	DataFrame/combine/dataframe_combine.c DataFrame/combine/dataframe_combine_test.c \
	DataFrame/reshape/dataframe_reshape.c DataFrame/reshape/dataframe_reshape_test.c \
	Series/series.c \
	../../DataStructures/src/DynamicArray/dynamic_array.c

OBJ = $(SRC:.c=.o)

all: myprogram

myprogram: $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) -o myprogram

clean:
	rm -f myprogram $(OBJ)

# ----------------------------------------
# ASAN build
# ----------------------------------------
# This adds -fsanitize=address (and some recommended flags).
# You can build with: make asan
# It produces an executable named 'myprogram' but with ASAN enabled.
ASANFLAGS = -fsanitize=address -g -O1 -fno-omit-frame-pointer

asan: CFLAGS += $(ASANFLAGS)
asan: clean myprogram
