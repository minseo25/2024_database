from mysql.connector import connect
import pandas as pd

class Messages():
    """Class that contains the messages to be printed to the user."""

    @staticmethod
    def successDBInit():
        print("Database successfully initialized")
    @staticmethod
    def successUserAdd():
        print("User successfully added")
    @staticmethod
    def successDVDAdd():
        print("DVD successfully added")
    @staticmethod
    def successUserRemove():
        print("User successfully removed")
    @staticmethod
    def successDVDRemove():
        print("DVD successfully removed")
    @staticmethod
    def successDVDCheckout():
        print("DVD successfully checked out")
    @staticmethod
    def successDVDReturnAndRate():
        print("DVD successfully returned and rated")
    @staticmethod
    def exitMsg():
        print("Bye!")
    @staticmethod
    def titleLengthError():
        print("Title length should range from 1 to 100 characters")
    @staticmethod
    def directorLengthError():
        print("Director length should range from 1 to 50 characters")
    @staticmethod
    def dvdAlreadyExistsError(title, director):
        print(f"DVD ({title}, {director}) already exists")
    @staticmethod
    def usernameLengthError():
        print("Username length should range from 1 to 50 characters")
    @staticmethod
    def dvdNotExistError(d_id):
        print(f"DVD {d_id} does not exist")
    @staticmethod
    def dvdDeleteError():
        print("Cannot delete a DVD that is currently borrowed")
    @staticmethod
    def userNotExistError(u_id):
        print(f"User {u_id} does not exist")
    @staticmethod
    def userDeleteError():
        print("Cannot delete a user with borrowed DVDs")
    @staticmethod
    def dvdOutOfStockError():
        print("Cannot check out a DVD that is out of stock")
    @staticmethod
    def userExceededBorrowingLimitError(u_id):
        print(f"User {u_id} exceeded the maximum borrowing limit")
    @staticmethod
    def invalidRatingError():
        print("Rating should range from 1 to 5 integer value")
    @staticmethod
    def dvdNotBorrowedError():
        print("Cannot return and rate a DVD that is not currently borrowed for this user")
    @staticmethod
    def userDuplicateError(name, age):
        print(f"({name}, {age}) already exists")
    @staticmethod
    def ageError():
        print("Age should be a positive integer")
    @staticmethod
    def simultaneousBorrowingError():
        print("User cannot borrow same DVD simultaneously")
    @staticmethod
    def failMatchError():
        print("Cannot find any matching results")
    

# mysql db connection
while True:
    try:
        connection = connect(
            host='astronaut.snu.ac.kr',
            port=7000,
            user='DB_MINSEO25',
            password='DB_MINSEO25',
            db='DB_MINSEO25',
            charset='utf8'
        )
        break
    except:
        continue
cursor = connection.cursor()

def initialize_database():
    # drop table if exists (create 순서 반대로 삭제, 참조관계 때문에)
    drop_queries = [
        "DROP TABLE IF EXISTS Rating",
        "DROP TABLE IF EXISTS Loan",
        "DROP TABLE IF EXISTS DVD",
        "DROP TABLE IF EXISTS Member",
        "DROP TABLE IF EXISTS Director"
    ]
    for query in drop_queries:
        cursor.execute(query)
    connection.commit()

    # create table
    create_queries = [
        """
        CREATE TABLE Director (
            dir_name VARCHAR(50) PRIMARY KEY,
            dir_rating FLOAT DEFAULT NULL,
            total_loans INT DEFAULT 0
        )
        """,
        """
        CREATE TABLE Member (
            u_id INT PRIMARY KEY AUTO_INCREMENT,
            u_name VARCHAR(50) NOT NULL,
            u_age INT NOT NULL CHECK (u_age > 0),
            loan_count INT DEFAULT 0,
            avg_rating FLOAT DEFAULT NULL,
            UNIQUE (u_name, u_age)
        )
        """,
        """
        CREATE TABLE DVD (
            d_id INT PRIMARY KEY AUTO_INCREMENT,
            d_title VARCHAR(100) NOT NULL,
            dir_name VARCHAR(50) NOT NULL,
            available_qty INT DEFAULT 2 CHECK (available_qty BETWEEN 0 AND 2),
            total_loans INT DEFAULT 0,
            avg_rating FLOAT DEFAULT NULL,
            FOREIGN KEY (dir_name) REFERENCES Director(dir_name)
        )
        """,
        """
        CREATE TABLE Loan (
            loan_id INT PRIMARY KEY AUTO_INCREMENT,
            u_id INT NOT NULL,
            d_id INT NOT NULL,
            FOREIGN KEY (u_id) REFERENCES Member(u_id) ON DELETE RESTRICT,
            FOREIGN KEY (d_id) REFERENCES DVD(d_id) ON DELETE RESTRICT,
            UNIQUE (u_id, d_id)
        )
        """,
        """
        CREATE TABLE Rating (
            rating_id INT PRIMARY KEY AUTO_INCREMENT,
            u_id INT NOT NULL,
            d_id INT NOT NULL,
            rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
            FOREIGN KEY (u_id) REFERENCES Member(u_id) ON DELETE CASCADE,
            FOREIGN KEY (d_id) REFERENCES DVD(d_id) ON DELETE CASCADE
        )
        """
    ]
    for query in create_queries:
        cursor.execute(query)
    connection.commit()

    # read data from data.csv
    try:
        data = pd.read_csv('data.csv', encoding='utf-8')
    except FileNotFoundError:
        print("Error: data.csv file not found.")
        return
    
    # drop unnamed column
    data = data.drop(columns=['Unnamed: 0'])

    # rename columns and convert types
    data.columns = ['d_id', 'd_title', 'd_name', 'u_id', 'u_name', 'u_age', 'rating']
    data['d_id'] = data['d_id'].astype(int)
    data['u_id'] = data['u_id'].astype(int)
    data['u_age'] = data['u_age'].astype(int)
    data['rating'] = data['rating'].astype(int)

    # remove duplicates
    directors = data[['d_name']].drop_duplicates()
    dvds = data[['d_id', 'd_title', 'd_name']].drop_duplicates()
    members = data[['u_id', 'u_name', 'u_age']].drop_duplicates()
    ratings = data[['u_id', 'd_id', 'rating']]

    # insert data into Director table
    # Director table의 dir_name, total_loans는 추후 접근 시 초기화
    for _, row in directors.iterrows():
        dir_name = row['d_name']
        cursor.execute("INSERT INTO Director (dir_name) VALUES (%s)", (dir_name,))
    connection.commit()

    # insert data into DVD table
    for _, row in dvds.iterrows():
        d_id = int(row['d_id'])
        d_title = row['d_title']
        dir_name = row['d_name']
        cursor.execute("INSERT INTO DVD (d_id, d_title, dir_name) VALUES (%s, %s, %s)", (d_id, d_title, dir_name))
    connection.commit()

    # insert data into Member table
    for _, row in members.iterrows():
        u_id = int(row['u_id'])
        u_name = row['u_name']
        u_age = int(row['u_age'])
        cursor.execute("INSERT INTO Member (u_id, u_name, u_age) VALUES (%s, %s, %s)", (u_id, u_name, u_age))
    connection.commit()

    # insert data into Rating table
    for _, row in ratings.iterrows():
        u_id = int(row['u_id'])
        d_id = int(row['d_id'])
        rating = int(row['rating'])
        cursor.execute("INSERT INTO Rating (u_id, d_id, rating) VALUES (%s, %s, %s)", (u_id, d_id, rating))
    connection.commit()

    # update total_loans and avg_rating in DVD table
    update_query = """
    UPDATE DVD d
    LEFT JOIN (
        SELECT d_id, count(*) AS loan_count, avg(rating) AS avg_rating
        FROM Rating
        GROUP BY d_id
    ) r ON d.d_id = r.d_id
    SET d.total_loans = r.loan_count, d.avg_rating = IFNULL(r.avg_rating, NULL)
    """
    cursor.execute(update_query)
    connection.commit()

    # update loan_count and avg_rating in Member table
    update_query = """
    UPDATE Member m
    LEFT JOIN (
        SELECT u_id, count(*) AS loan_count, avg(rating) AS avg_rating
        FROM Rating
        GROUP BY u_id
    ) r ON m.u_id = r.u_id
    SET m.loan_count = r.loan_count, m.avg_rating = IFNULL(r.avg_rating, NULL)
    """
    cursor.execute(update_query)
    connection.commit()

    # update total_loans and avg_rating in Director table
    update_query = """
    UPDATE Director d
    LEFT JOIN (
        SELECT dir_name, 
               SUM(total_loans) AS total_loans, 
               AVG(avg_rating) AS dir_rating
        FROM DVD
        GROUP BY dir_name
    ) dv ON d.dir_name = dv.dir_name
    SET d.total_loans = dv.total_loans, d.dir_rating = IFNULL(dv.dir_rating, NULL)
    """
    cursor.execute(update_query)
    connection.commit()

    Messages.successDBInit()

def reset():
    # reset database (ask user for confirmation)
    res = input("Are you sure to reset the database? (y/n): ")
    if res == 'y':
        initialize_database()

def print_DVDs():
    header = "id".ljust(5) + "title".ljust(100) + "director".ljust(50) + "avg.rating".ljust(15) + "cumul_rent_cnt".ljust(20) + "quantity".ljust(15)
    separator = "-" * len(header)

    # print header and separator
    print(separator)
    print(header)
    print(separator)

    # read data from DVD table
    query = """
    SELECT d_id, d_title, dir_name, avg_rating, total_loans, available_qty
    FROM DVD
    ORDER BY IFNULL(avg_rating, 0) DESC, total_loans DESC
    """
    cursor.execute(query)
    results = cursor.fetchall()

    # print data
    for result in results:
        d_id, d_title, dir_name, avg_rating, total_loans, available_qty = result
        avg_rating_str = f"{avg_rating:.3f}" if avg_rating is not None else "None"
        print(f"{str(d_id).ljust(5)}"
              f"{d_title.ljust(100)}"
              f"{dir_name.ljust(50)}"
              f"{avg_rating_str.ljust(15)}"
              f"{str(total_loans).ljust(20)}"
              f"{str(available_qty).ljust(15)}")
    
    print(separator)

def print_users():
    header = "id".ljust(5) + "name".ljust(50) + "age".ljust(8) + "avg.rating".ljust(15) + "cumul_rent_cnt".ljust(20)
    separator = "-" * len(header)

    # print header and separator
    print(separator)
    print(header)
    print(separator)

    # read data from Member table
    query = """
    SELECT u_id, u_name, u_age, avg_rating, loan_count
    FROM Member
    ORDER BY u_id ASC
    """
    cursor.execute(query)
    results = cursor.fetchall()

    # print data
    for result in results:
        u_id, u_name, u_age, avg_rating, loan_count = result
        avg_rating_str = f"{avg_rating:.3f}" if avg_rating is not None else "None"
        print(f"{str(u_id).ljust(5)}"
              f"{u_name.ljust(50)}"
              f"{str(u_age).ljust(8)}"
              f"{avg_rating_str.ljust(15)}"
              f"{str(loan_count).ljust(20)}")
    
    print(separator)

def insert_DVD():
    title = input('DVD title: ')
    director = input('DVD director: ')
    has_error = False

    # input validation
    if len(title) < 1 or len(title) > 100:
        Messages.titleLengthError()
        has_error = True
    if len(director) < 1 or len(director) > 50:
        Messages.directorLengthError()
        has_error = True
    
    # check if (title, director) already exists, case insensitive
    query = """
    SELECT * FROM DVD WHERE LOWER(d_title) = LOWER(%s) AND LOWER(dir_name) = LOWER(%s)
    """
    cursor.execute(query, (title, director))
    if cursor.fetchone():
        Messages.dvdAlreadyExistsError(title, director)
        has_error = True

    # 모든 에러를 다 출력해주기 위해 return 시점을 여기로 옮김
    if has_error:
        return
    
    # insert new Director if not exists
    query = """
    INSERT IGNORE INTO Director (dir_name) VALUES (%s)
    """
    cursor.execute(query, (director,))
    
    # insert new DVD (d_id is auto incremented)
    query = """
    INSERT INTO DVD (d_title, dir_name, available_qty, total_loans, avg_rating) VALUES (%s, %s, 2, 0, NULL)
    """
    cursor.execute(query, (title, director))
    connection.commit()
    Messages.successDVDAdd()

def remove_DVD():
    try:
        DVD_id = int(input('DVD ID: '))
    except ValueError:
        print("Input Error")
        return
    
    has_error = False
    
    # check if DVD exists
    query = """
    SELECT * FROM DVD WHERE d_id = %s
    """
    cursor.execute(query, (DVD_id,))
    if not cursor.fetchone():
        Messages.dvdNotExistError(DVD_id)
        has_error = True
    
    # check if DVD is currently borrowed
    query = """
    SELECT * FROM Loan WHERE d_id = %s
    """
    cursor.execute(query, (DVD_id,))
    if cursor.fetchone():
        Messages.dvdDeleteError()
        has_error = True
    
    # 모든 에러를 다 출력해주기 위해 return 시점을 여기로 옮김
    if has_error:
        return
    
    # delete DVD, Rating table의 레코드들도 delete on cascade 옵션으로 같이 삭제됨
    query = """
    DELETE FROM DVD WHERE d_id = %s
    """
    cursor.execute(query, (DVD_id,))
    connection.commit()
    Messages.successDVDRemove()

def insert_user():
    try:
        name = input('User name: ')
        age = int(input('User age: '))
    except ValueError:
        print("Input Error")
        return
    
    has_error = False

    # input validation
    if len(name) < 1 or len(name) > 50:
        Messages.usernameLengthError()
        has_error = True
    if age <= 0:
        Messages.ageError()
        has_error = True
    
    # check if (name, age) already exists, case insensitive
    query = """
    SELECT * FROM Member WHERE LOWER(u_name) = LOWER(%s) AND u_age = %s
    """
    cursor.execute(query, (name, age))
    if cursor.fetchone():
        Messages.userDuplicateError(name, age)
        has_error = True
    
    # 모든 에러를 다 출력해주기 위해 return 시점을 여기로 옮김
    if has_error:
        return
    
    # insert new user (u_id is auto incremented)
    query = """
    INSERT INTO Member (u_name, u_age, loan_count, avg_rating) VALUES (%s, %s, 0, NULL)
    """
    cursor.execute(query, (name, age))
    connection.commit()
    Messages.successUserAdd()

def remove_user():
    try:
        user_id = int(input('User ID: '))
    except ValueError:
        print("Input Error")
        return
    
    has_error = False

    # check if user exists
    query = """
    SELECT * FROM Member WHERE u_id = %s
    """
    cursor.execute(query, (user_id,))
    if not cursor.fetchone():
        Messages.userNotExistError(user_id)
        has_error = True
    
    # check if user has borrowed DVDs
    query = """
    SELECT * FROM Loan WHERE u_id = %s
    """
    cursor.execute(query, (user_id,))
    if cursor.fetchone():
        Messages.userDeleteError()
        has_error = True
    
    # 모든 에러를 다 출력해주기 위해 return 시점을 여기로 옮김
    if has_error:
        return
    
    # delete user, Rating table의 레코드들도 delete on cascade 옵션으로 같이 삭제됨
    query = """
    DELETE FROM Member WHERE u_id = %s
    """
    cursor.execute(query, (user_id,))
    connection.commit()
    Messages.successUserRemove()

def checkout_DVD():
    try:
        user_id = int(input('User ID: '))
        DVD_id = int(input('DVD ID: '))
    except ValueError:
        print("Input Error")
        return
    
    # check if DVD exists
    query = """
    SELECT d_id, d_title, dir_name, avg_rating, total_loans, available_qty FROM DVD WHERE d_id = %s
    """
    cursor.execute(query, (DVD_id,))
    result = cursor.fetchone()

    if result is None:
        Messages.dvdNotExistError(DVD_id)
        return
    else:
        _, _, _, _, _, available_qty = result
        if available_qty <= 0:
            Messages.dvdOutOfStockError()
            return
    
    # check if user exists
    query = """
    SELECT * FROM Member WHERE u_id = %s
    """
    cursor.execute(query, (user_id,))
    if not cursor.fetchone():
        Messages.userNotExistError(user_id)
        return
    
    # check if user is borrowing same DVD simultaneously
    query = """
    SELECT * FROM Loan WHERE u_id = %s AND d_id = %s
    """
    cursor.execute(query, (user_id, DVD_id))
    if cursor.fetchone():
        Messages.simultaneousBorrowingError()
        return
    
    # check if user has already borrowed 3 DVDs
    query = """
    SELECT * FROM Loan WHERE u_id = %s
    """
    cursor.execute(query, (user_id,))
    if len(cursor.fetchall()) >= 3:
        Messages.userExceededBorrowingLimitError(user_id)
        return
    
    # checkout DVD (loan_id is auto incremented)
    query = """
    INSERT INTO Loan (u_id, d_id) VALUES (%s, %s)
    """
    cursor.execute(query, (user_id, DVD_id))
    connection.commit()

    # update available_qty in DVD table
    query = """
    UPDATE DVD SET available_qty = available_qty - 1 WHERE d_id = %s
    """
    cursor.execute(query, (DVD_id,))
    connection.commit()

    Messages.successDVDCheckout()

def return_and_rate_DVD():
    try:
        user_id = int(input('User ID: '))
        DVD_id = int(input('DVD ID: '))
        rating = int(input('Ratings (1~5): '))
    except ValueError:
        print("Input Error")
        return
    
    # check if DVD exists
    query = """
    SELECT avg_rating, total_loans FROM DVD WHERE d_id = %s
    """
    cursor.execute(query, (DVD_id,))
    dvd = cursor.fetchone()
    if not dvd:
        Messages.dvdNotExistError(DVD_id)
        return
    
    # check if user exists
    query = """
    SELECT avg_rating, loan_count FROM Member WHERE u_id = %s
    """
    cursor.execute(query, (user_id,))
    member = cursor.fetchone()
    if not member:
        Messages.userNotExistError(user_id)
        return

    # check if rating is valid
    if rating < 1 or rating > 5:
        Messages.invalidRatingError()
        return
    
    # check if DVD is currently borrowed by the user
    query = """
    SELECT * FROM Loan WHERE u_id = %s AND d_id = %s
    """
    cursor.execute(query, (user_id, DVD_id))
    if not cursor.fetchone():
        Messages.dvdNotBorrowedError()
        return
    
    # remove record from Loan table
    query = """
    DELETE FROM Loan WHERE u_id = %s AND d_id = %s
    """
    cursor.execute(query, (user_id, DVD_id))
    connection.commit()

    # insert record to Rating table
    query = """
    INSERT INTO Rating (u_id, d_id, rating) VALUES (%s, %s, %s)
    """
    cursor.execute(query, (user_id, DVD_id, rating))
    connection.commit()

    # update available_qty, total_loans, avg_rating in DVD table
    prev_avg_rating, prev_total_loans = dvd
    prev_avg_rating = 0.0 if prev_avg_rating is None else float(prev_avg_rating)
    new_total_loans = prev_total_loans + 1
    new_avg_rating = (prev_avg_rating * prev_total_loans + int(rating)) / new_total_loans
    query = """
    UPDATE DVD SET available_qty = available_qty + 1, total_loans = %s, avg_rating = %s WHERE d_id = %s
    """
    cursor.execute(query, (new_total_loans, new_avg_rating, DVD_id))
    connection.commit()

    # update loan_count and avg_rating in Member table
    prev_avg_rating, prev_loan_count = member
    prev_avg_rating = 0.0 if prev_avg_rating is None else float(prev_avg_rating)
    new_loan_count = prev_loan_count + 1
    new_avg_rating = (prev_avg_rating * prev_loan_count + int(rating)) / new_loan_count
    query = """
    UPDATE Member SET loan_count = %s, avg_rating = %s WHERE u_id = %s
    """
    cursor.execute(query, (new_loan_count, new_avg_rating, user_id))
    connection.commit()

    Messages.successDVDReturnAndRate()

def print_borrowing_status_for_user():
    try:
        user_id = int(input('User ID: '))
    except ValueError:
        print("Input Error")
        return
    
    # check if user exists
    query = """
    SELECT * FROM Member WHERE u_id = %s
    """
    cursor.execute(query, (user_id,))
    if not cursor.fetchone():
        Messages.userNotExistError(user_id)
        return
    
    header = "id".ljust(5) + "title".ljust(100) + "director".ljust(50) + "avg.rating".ljust(15)
    separator = "-" * len(header)

    # print header and separator
    print(separator)
    print(header)
    print(separator)

    # get dvd_ids borrowed by the user
    query = """
    SELECT d_id FROM Loan WHERE u_id = %s ORDER BY d_id ASC
    """
    cursor.execute(query, (user_id,))
    results = cursor.fetchall()

    # print data
    for result in results:
        dvd_id = result[0]
        query = """
        SELECT d_id, d_title, dir_name, avg_rating FROM DVD WHERE d_id = %s
        """
        cursor.execute(query, (dvd_id,))
        d_id, d_title, dir_name, avg_rating = cursor.fetchone()
        avg_rating_str = f"{avg_rating:.3f}" if avg_rating is not None else "None"
        print(f"{str(d_id).ljust(5)}"
              f"{d_title.ljust(100)}"
              f"{dir_name.ljust(50)}"
              f"{avg_rating_str.ljust(15)}")
    
    print(separator)

def search_DVD():
    search_query = input('Query: ')
    
    # search by DVD title (case insensitive)
    query = """
    SELECT d_id, d_title, dir_name, avg_rating, total_loans, available_qty
    FROM DVD
    WHERE LOWER(d_title) LIKE LOWER(%s)
    ORDER BY IFNULL(avg_rating, 0) DESC, total_loans DESC
    """
    cursor.execute(query, (f'%{search_query}%',))
    results = cursor.fetchall()
    
    if not results:
        Messages.failMatchError()
        return
    
    header = "id".ljust(5) + "title".ljust(100) + "director".ljust(50) + "avg.rating".ljust(15) + "cumul_rent_cnt".ljust(20) + "quantity".ljust(15)
    separator = "-" * len(header)

    # print header and separator
    print(separator)
    print(header)
    print(separator)

    # print data
    for result in results:
        d_id, d_title, dir_name, avg_rating, total_loans, available_qty = result
        avg_rating_str = f"{avg_rating:.3f}" if avg_rating is not None else "None"
        print(f"{str(d_id).ljust(5)}"
              f"{d_title.ljust(100)}"
              f"{dir_name.ljust(50)}"
              f"{avg_rating_str.ljust(15)}"
              f"{str(total_loans).ljust(20)}"
              f"{str(available_qty).ljust(15)}")
    
    print(separator)

def _update_director_info(dir_name):
    # search_director()나 recommend_popularity() 호출 시 (이 외에는 감독정보 업데이트 필요 없음)
    # 감독별로 감독평점(DVD 평균 평점의 평균 평점..), 누적 대출횟수 업데이트하고
    # 감독의 info를 반환한다
    total_rating = 0.0
    rating_count = 0
    dir_total_loans = 0
    dvd_lists = []
    
    query = """
    SELECT d_title, avg_rating, total_loans
    FROM DVD
    WHERE dir_name = %s
    """
    cursor.execute(query, (dir_name,))
    results = cursor.fetchall()

    for result in results:
        d_title, avg_rating, total_loans = result
        if avg_rating is not None:
            total_rating += avg_rating
            rating_count += 1
        dir_total_loans += total_loans
        dvd_lists.append(d_title)
    
    # 감독의 DVD 목록이 없거나 DVD 평균 평점이 하나도 없으면 감독평점 업데이트 안함
    if len(results) == 0 or rating_count == 0:
        dir_rating = None
    else:
        dir_rating = total_rating / rating_count
    
    query = """
    UPDATE Director SET dir_rating = %s, total_loans = %s WHERE dir_name = %s
    """
    cursor.execute(query, (dir_rating, dir_total_loans, dir_name))
    connection.commit()
    
    return dir_rating, dir_total_loans, dvd_lists

def search_director():
    search_query = input('Query: ')
    
    # search by director name (case insensitive)
    query = """
    SELECT dir_name FROM Director WHERE LOWER(dir_name) LIKE LOWER(%s)
    """
    cursor.execute(query, (f'%{search_query}%',))
    results = cursor.fetchall()

    if not results:
        Messages.failMatchError()
        return
    
    # update director info and receive their DVD lists
    director_dvd_lists = {}
    for result in results:
        dir_name = result[0]
        _, _, dvd_lists = _update_director_info(dir_name)
        dvd_lists.sort()
        director_dvd_lists[dir_name] = dvd_lists

    header = "director".ljust(50) + "director_rating".ljust(20) + "cumul_rent_cnt".ljust(20) + "all_movies".ljust(100)
    separator = "-" * len(header)

    # print header and separator
    print(separator)
    print(header)
    print(separator)

    # search by director name (order by director_rating desc, cumul_rent_cnt desc)
    query = """
    SELECT dir_name, dir_rating, total_loans
    FROM Director
    WHERE LOWER(dir_name) LIKE LOWER(%s)
    ORDER BY dir_rating DESC, total_loans DESC
    """
    cursor.execute(query, (f'%{search_query}%',))
    results = cursor.fetchall()

    # print data
    for result in results:
        dir_name, dir_rating, total_loans = result
        dir_rating_str = f"{dir_rating:.3f}" if dir_rating is not None else "None"
        print(f"{dir_name.ljust(50)}"
              f"{str(dir_rating_str).ljust(20)}"
              f"{str(total_loans).ljust(20)}"
              f"{str(director_dvd_lists[dir_name])}")
    
    print(separator)

def recommend_popularity():
    try:
        user_id = int(input('User ID: '))
    except ValueError:
        print("Input Error")
        return
    
    # check if user exists
    query = """
    SELECT * FROM Member WHERE u_id = %s
    """
    cursor.execute(query, (user_id,))
    if not cursor.fetchone():
        Messages.userNotExistError(user_id)
        return
    
    # get d_ids that the user has rated
    query = """
    SELECT DISTINCT d_id FROM Rating WHERE u_id = %s
    """
    cursor.execute(query, (user_id,))
    results = cursor.fetchall()
    rated_d_ids = [result[0] for result in results]

    # update all info in Director table
    query = """
    SELECT dir_name FROM Director
    """
    cursor.execute(query)
    results = cursor.fetchall()
    dir_names = [result[0] for result in results]
    for dir_name in dir_names:
        _update_director_info(dir_name)

    header1 = "id".ljust(5) + "title".ljust(100) + "director".ljust(50) + "avg.rating".ljust(15) + "quantity".ljust(15)
    header2 = "id".ljust(5) + "title".ljust(100) + "director".ljust(50) + "cumul_rent_cnt".ljust(20) + "quantity".ljust(15)
    separator = "-" * len(header2)

    print(separator)
    print("Rating-based")
    print(separator)
    print(header1)
    print(separator)

    # rating-based recommendation
    query = """
    SELECT d.d_id, d.d_title, d.dir_name, d.avg_rating, d.available_qty
    FROM DVD d
    JOIN Director dir ON d.dir_name = dir.dir_name
    WHERE d.d_id NOT IN %s
    ORDER BY IFNULL(d.avg_rating, 0) DESC,
             d.total_loans DESC,
             IFNULL(dir.dir_rating, 0) DESC,
             d.d_id ASC
    LIMIT 1
    """
    if rated_d_ids:
        # user가 평가한 dvd 목록이 있을 경우 그 목록에 포함되지 않는 dvd 중에서 추천
        placeholders = ', '.join(['%s'] * len(rated_d_ids))
        query = query.replace('NOT IN %s', f'NOT IN ({placeholders})')
        cursor.execute(query, tuple(rated_d_ids))
    else:
        # user가 평가한 dvd 목록이 없을 경우 모든 dvd 중에서 추천 (NOT IN (-1))
        query = query.replace('NOT IN %s', 'NOT IN (-1)')
        cursor.execute(query)
    rating_based_result = cursor.fetchone()
    if rating_based_result:
        d_id, d_title, dir_name, avg_rating, available_qty = rating_based_result
        avg_rating_str = f"{avg_rating:.3f}" if avg_rating is not None else "None"
        print(f"{str(d_id).ljust(5)}"
            f"{d_title.ljust(100)}"
            f"{dir_name.ljust(50)}"
            f"{avg_rating_str.ljust(15)}"
            f"{str(available_qty).ljust(15)}")
    
    print(separator)
    print("Popularity-based")
    print(separator)
    print(header2)
    print(separator)

    # popularity-based recommendation
    query = """
    SELECT d.d_id, d.d_title, d.dir_name, d.total_loans, d.available_qty
    FROM DVD d
    JOIN Director dir ON d.dir_name = dir.dir_name
    WHERE d.d_id NOT IN %s
    ORDER BY d.total_loans DESC,
             IFNULL(d.avg_rating, 0) DESC,
             IFNULL(dir.dir_rating, 0) DESC,
             d.d_id ASC
    LIMIT 1
    """
    if rated_d_ids:
        # user가 평가한 dvd 목록이 있을 경우 그 목록에 포함되지 않는 dvd 중에서 추천
        placeholders = ', '.join(['%s'] * len(rated_d_ids))
        query = query.replace('NOT IN %s', f'NOT IN ({placeholders})')
        cursor.execute(query, tuple(rated_d_ids))
    else:
        # user가 평가한 dvd 목록이 없을 경우 모든 dvd 중에서 추천 (NOT IN (-1))
        query = query.replace('NOT IN %s', 'NOT IN (-1)')
        cursor.execute(query)
    popularity_based_result = cursor.fetchone()
    if popularity_based_result:
        d_id, d_title, dir_name, total_loans, available_qty = popularity_based_result
        print(f"{str(d_id).ljust(5)}"
            f"{d_title.ljust(100)}"
            f"{dir_name.ljust(50)}"
            f"{str(total_loans).ljust(20)}"
            f"{str(available_qty).ljust(15)}")

    print(separator)

def recommend_user_based():
    recommend_popularity()

def main():
    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all DVDs')
        print('3. print all users')
        print('4. insert a new DVD')
        print('5. remove a DVD')
        print('6. insert a new user')
        print('7. remove a user')
        print('8. check out a DVD')
        print('9. return and rate a DVD')
        print('10. print borrowing status of a user')
        print('11. search DVDs')
        print('12. search directors')
        print('13. recommend a DVD for a user using popularity-based method')
        print('14. recommend a DVD for a user using user-based collaborative filtering')
        print('15. exit')
        print('16. reset database')
        print('============================================================')
        try:
            menu = int(input('Select your action: '))
        except ValueError:
            print('Invalid action')
            continue

        if menu == 1:
            initialize_database()
        elif menu == 2:
            print_DVDs()
        elif menu == 3:
            print_users()
        elif menu == 4:
            insert_DVD()
        elif menu == 5:
            remove_DVD()
        elif menu == 6:
            insert_user()
        elif menu == 7:
            remove_user()
        elif menu == 8:
            checkout_DVD()
        elif menu == 9:
            return_and_rate_DVD()
        elif menu == 10:
            print_borrowing_status_for_user()
        elif menu == 11:
            search_DVD()
        elif menu == 12:
            search_director()
        elif menu == 13:
            recommend_popularity()
        elif menu == 14:
            recommend_user_based()
        elif menu == 15:
            Messages.exitMsg()
            cursor.close()
            connection.close()
            break
        elif menu == 16:
            reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()
