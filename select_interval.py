import psycopg2












if __name__ == "__main__":
    start = input("select starting date:").strip() 
    end = input("select ending date:").strip()


    select_interval(start,end)
