CC = gcc
CFLAGS = -Wall -Wextra -O2

SRC = \
	main.c \
	DataFrame/dataframe_core.c \
	DataFrame/dataframe_query.c \
	DataFrame/dataframe_print.c \
	DataFrame/dataframe_csv.c \
	DataFrame/dataframe_plot.c \
	DataFrame/dataframe_date.c \
	Series/series.c \
	../../DataStructures/src/DynamicArray/dynamic_array.c

OBJ = $(SRC:.c=.o)

all: myprogram

myprogram: $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) -o myprogram

clean:
	rm -f myprogram $(OBJ)
