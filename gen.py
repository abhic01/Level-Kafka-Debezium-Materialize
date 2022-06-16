from secrets import choice
import psycopg2
import random
import datetime
from psycopg2 import Error

# FUNCTION TO INSERT 'count' NUMBER OF RANDOM VALUES INTO THE PG DB
def insert(count : int) -> int:
    success_count = 0

    for j in range(count):
        try:
            lines = open("D:/Downloads/example data/random.txt").read().splitlines() 
            connection = psycopg2.connect(user="postgres", password="abhi", host="127.0.0.1", port="5432", database="consumer")
            cursor = connection.cursor()

            query = "INSERT INTO public.device_telemetry_bolt_operation_statistics VALUES (" + str(random.randint(20000, 20000 * count)) + ",'" + str(datetime.datetime.now().replace(microsecond=j)) + "',"
            choices = random.sample(lines, 11)
            for i in range(11):
                choices[i] = choices[i].split()
                if choices[i][i + 3].isdigit():
                    query += choices[i][i + 3]
                else:
                    query += "'" + choices[i][i + 3] + "'"
                if i < 10:
                    query += ","
            query += ");"

            cursor.execute(query)
            connection.commit()
            success_count += 1

        except (Exception, Error) as error:
            print(error)

        finally:
            if (connection):
                cursor.close()
                connection.close()

    return success_count

# FUNCTION TO UPDATE 'count' NUMBER OF RANDOM ROWS IN THE PG DB
def update(count : int) -> int:
    success_count = 0

    for j in range(count):
        try:
            lines = open("D:/Downloads/example data/random.txt").read().splitlines() 
            connection = psycopg2.connect(user="postgres", password="abhi", host="127.0.0.1", port="5432", database="consumer")
            cursor = connection.cursor()

            options = ["max_current", "average_current", "current_variance", "duration", "min_voltage"]
            query = "UPDATE public.device_telemetry_bolt_operation_statistics SET " + random.choice(options) +" = " + str(random.randint(50, 700)) + " WHERE id = " + str(random.randint(8673, 12697)) + ";"
            cursor.execute(query)
            connection.commit()
            success_count += 1

        except (Exception, Error) as error:
            print(error)

        finally:
            if (connection):
                cursor.close()
                connection.close()

    return success_count

# FUNCTION TO DELETE 'count' NUMBER OF RANDOM ROWS IN THE PG DB
def delete(count : int) -> int:
    success_count = 0

    for j in range(count):
        try:
            lines = open("D:/Downloads/example data/random.txt").read().splitlines() 
            connection = psycopg2.connect(user="postgres", password="abhi", host="127.0.0.1", port="5432", database="consumer")
            cursor = connection.cursor()

            query = "DELETE FROM public.device_telemetry_bolt_operation_statistics WHERE id = " + str(random.randint(8673, 12697)) + ";"
            cursor.execute(query)
            connection.commit()
            success_count += 1

        except (Exception, Error) as error:
            print(error)

        finally:
            if (connection):
                cursor.close()
                connection.close()

    return success_count

# FUNCTION TO CLEAN-UP ALL RANDOM INSERTS MADE
def cleanup():
    try:
        lines = open("D:/Downloads/example data/random.txt").read().splitlines() 
        connection = psycopg2.connect(user="postgres", password="abhi", host="127.0.0.1", port="5432", database="consumer")
        cursor = connection.cursor()
        cursor.execute("DELETE FROM public.device_telemetry_bolt_operation_statistics WHERE id > 20000;")
        connection.commit()

    except (Exception, Error) as error:
        print(error)

    finally:
        if (connection):
            cursor.close()
            connection.close()