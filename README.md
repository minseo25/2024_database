## PRJ 1. DBMS 구현하기

mac의 경우 아래와 같이 dependencies를 설치할 수 있다
```
brew install berkeley-db@5
export BERKELEYDB_DIR=$(brew --prefix berkeley-db@5)
pip install -r requirements.txt
```
[TODO]
* lark api를 통해 SQL문을 파싱할 수 있는 SQL 파서를 구현 (grammar.lark 파일 참고)
* berkelydb api와 파서를 통해 DBMS를 구현
  - 스키마를 저장하고 관리
  - 스키마에 데이터 추가, 검색, 삭제, 수정

[스키마 설계 및 데이터 관리 방법]
```
[key]
schema:<table_name>
[value]
{
    "columns": ["column_name", "column_name2", ...],
    "columns_metadata": {
        "column_name" : {
            "type": ~~~, // (char(n), int, date)
            "not_null": ~~, // (True, False)
            "primary_key": ~~, // (True, False)
            "foreign_key": ~~, // (True, False)
        },
        ...
    }
}

[key]
data:<table_name>
[value]
[
    {
        "column_name": ~~ ,
        "column_name2": ~~,
        "column_name3": ~~,
    },
    ...
]

[key]
reference:<table_name>:<table_name2>
[value]
{
    "t1의 column": "t2의 column",
    ...
}
```
* One DB-Multi Schema 방식
  - 하나의 DB파일에 복수의 스키마를 관리하는 방법 채택
  - 메타데이터와 데이터를 하나의 DB 파일에 통합해 저장하는 방법 채택 (접두사 활용)
* 메타데이터 저장 시 default 컬럼 순서도 저장
* t1에서 foreign key 로 t2 테이블을 reference하는 관계 역시 저장


## PRJ 2. MySQL 활용 어플리케이션 구현하기

* Python과 MySQL을 이용한 응용 시스템 구현
* DVD 정보,회원 정보 삽입 / 삭제 / 출력, 회원이 DVD를 대출, 반납과 평점 부여, 회원을 위한 DVD 추천
