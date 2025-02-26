CC = gcc
CFLAGS = -Wall -Wextra -O2

SRC = \
	main.c \
	DataFrame/core/dataframe_core.c DataFrame/core/dataframe_core_test.c \
	DataFrame/query/dataframe_query.c DataFrame/query/dataframe_query_test.c \
	DataFrame/print/dataframe_print.c DataFrame/print/dataframe_print_test.c \
	DataFrame/csv/dataframe_csv.c DataFrame/csv/dataframe_csv_test.c \
	DataFrame/plot/dataframe_plot.c \
	DataFrame/date/dataframe_date.c \
	Series/series.c \
	../../DataStructures/src/DynamicArray/dynamic_array.c

OBJ = $(SRC:.c=.o)

all: myprogram

myprogram: $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) -o myprogram

clean:
	rm -f myprogram $(OBJ)
