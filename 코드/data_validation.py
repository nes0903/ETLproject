import mysql.connector
import logging
from datetime import datetime
import os
import time

# 로그 설정
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 데이터베이스 연결 설정
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'kopo',
    'password': '1234',
    'database': 'kopo'
}

EC2_DB_CONFIG = {
    'host': '54.180.237.238',
    'port': 3306,
    'user': 'kopo',
    'password': '1234',
    'database': 'kopo2'
}

def connect_with_retry(config, max_retries=3):
    """데이터베이스 연결 재시도 함수"""
    for i in range(max_retries):
        try:
            connection = mysql.connector.connect(**config)
            return connection
        except mysql.connector.Error as e:
            if i == max_retries - 1:
                logger.error(f"데이터베이스 연결 실패: {str(e)}")
                raise
            logger.warning(f"데이터베이스 연결 실패, {i+1}번째 재시도...")
            time.sleep(2)

def check_row_count(cursor, table_name):
    """행 개수 검증"""
    try:
        query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        logger.info(f"{table_name} 테이블 행 개수: {count}")
        return count
    except mysql.connector.Error as e:
        logger.error(f"{table_name} 테이블 행 개수 검증 실패: {str(e)}")
        raise

def check_aggregation(cursor):
    """집계 검증 - 제품유형별 위해상품 여부 합계"""
    try:
        query = """
        SELECT 
            제품유형,
            COUNT(*) as 총개수,
            SUM(위해상품여부) as 위해상품수
        FROM 위해상품_위험도_분석
        GROUP BY 제품유형
        ORDER BY 위해상품수 DESC
        """
        cursor.execute(query)
        results = cursor.fetchall()
        logger.info("\n제품유형별 위해상품 현황:")
        for row in results:
            logger.info(f"제품유형: {row[0]}, 총개수: {row[1]}, 위해상품수: {row[2]}")
        return results
    except mysql.connector.Error as e:
        logger.error(f"집계 검증 실패: {str(e)}")
        raise

def check_duplicates(cursor):
    """중복 데이터 검증"""
    try:
        query = """
        SELECT 
            doc_no,
            doc_cycl,
            COUNT(*) as count
        FROM 위해상품_위험도_분석
        GROUP BY doc_no, doc_cycl
        HAVING COUNT(*) > 1
        """
        cursor.execute(query)
        duplicates = cursor.fetchall()
        if duplicates:
            logger.warning("\n중복 데이터 발견:")
            for row in duplicates:
                logger.warning(f"doc_no: {row[0]}, doc_cycl: {row[1]}, 중복횟수: {row[2]}")
        else:
            logger.info("\n중복 데이터 없음")
        return duplicates
    except mysql.connector.Error as e:
        logger.error(f"중복 데이터 검증 실패: {str(e)}")
        raise

def check_null_values(cursor):
    """NULL 값 검증"""
    try:
        query = """
        SELECT 
            COUNT(*) as total_rows,
            SUM(CASE WHEN 제품명 IS NULL THEN 1 ELSE 0 END) as null_제품명,
            SUM(CASE WHEN 제조국 IS NULL THEN 1 ELSE 0 END) as null_제조국,
            SUM(CASE WHEN 제품유형 IS NULL THEN 1 ELSE 0 END) as null_제품유형
        FROM 위해상품_위험도_분석
        """
        cursor.execute(query)
        result = cursor.fetchone()
        logger.info("\nNULL 값 검증 결과:")
        logger.info(f"총 행 수: {result[0]}")
        logger.info(f"제품명 NULL 수: {result[1]}")
        logger.info(f"제조국 NULL 수: {result[2]}")
        logger.info(f"제품유형 NULL 수: {result[3]}")
        return result
    except mysql.connector.Error as e:
        logger.error(f"NULL 값 검증 실패: {str(e)}")
        raise

def check_data_range(cursor):
    """데이터 범위 검증 - 위험도등급"""
    try:
        query = """
        SELECT 
            위험도등급,
            COUNT(*) as count
        FROM 위해상품_위험도_분석
        GROUP BY 위험도등급
        """
        cursor.execute(query)
        results = cursor.fetchall()
        logger.info("\n위험도등급 분포:")
        for row in results:
            logger.info(f"등급: {row[0]}, 개수: {row[1]}")
        
        # 허용되지 않은 값 확인
        invalid_query = """
        SELECT COUNT(*)
        FROM 위해상품_위험도_분석
        WHERE 위험도등급 NOT IN ('위험', '주의', '관심', '정상')
        """
        cursor.execute(invalid_query)
        invalid_count = cursor.fetchone()[0]
        if invalid_count > 0:
            logger.warning(f"\n허용되지 않은 위험도등급 발견: {invalid_count}개")
        else:
            logger.info("\n모든 위험도등급이 유효함")
        return results
    except mysql.connector.Error as e:
        logger.error(f"데이터 범위 검증 실패: {str(e)}")
        raise

try:
    # 데이터베이스 연결
    local_connection = connect_with_retry(LOCAL_DB_CONFIG)
    local_cursor = local_connection.cursor()
    logger.info("로컬 데이터베이스 연결 성공")
    
    ec2_connection = connect_with_retry(EC2_DB_CONFIG)
    ec2_cursor = ec2_connection.cursor()
    logger.info("EC2 데이터베이스 연결 성공")
    
    # 1. 행 개수 검증
    logger.info("\n=== 행 개수 검증 시작 ===")
    local_counts = {
        '위해상품': check_row_count(local_cursor, '위해상품'),
        '위해상품부적합검사': check_row_count(local_cursor, '위해상품부적합검사'),
        '위해상품업체': check_row_count(local_cursor, '위해상품업체')
    }
    
    ec2_counts = {
        '위해상품_위험도_분석': check_row_count(ec2_cursor, '위해상품_위험도_분석'),
        '위해상품_데이터셋': check_row_count(ec2_cursor, '위해상품_데이터셋')
    }
    
    # 2. 집계 검증
    logger.info("\n=== 집계 검증 시작 ===")
    check_aggregation(ec2_cursor)
    
    # 3. 중복 데이터 검증
    logger.info("\n=== 중복 데이터 검증 시작 ===")
    check_duplicates(ec2_cursor)
    
    # 4. NULL 값 검증
    logger.info("\n=== NULL 값 검증 시작 ===")
    check_null_values(ec2_cursor)
    
    # 5. 데이터 범위 검증
    logger.info("\n=== 데이터 범위 검증 시작 ===")
    check_data_range(ec2_cursor)
    
    # 검증 결과 이력 기록
    insert_history_query = """
        INSERT INTO 데이터셋_최신화_이력 
        (process_name, status, error_message)
        VALUES (%s, %s, %s)
    """
    ec2_cursor.execute(insert_history_query, ("데이터 검증", "성공", None))
    ec2_connection.commit()
    logger.info("\n검증 결과 이력 기록 완료")
    
except Exception as e:
    # 실패 이력 기록
    try:
        insert_history_query = """
            INSERT INTO 데이터셋_최신화_이력 
            (process_name, status, error_message)
            VALUES (%s, %s, %s)
        """
        ec2_cursor.execute(insert_history_query, ("데이터 검증", "실패", str(e)))
        ec2_connection.commit()
    except:
        pass
    logger.error(f"검증 중 오류 발생: {str(e)}")
    raise e
    
finally:
    # 연결 종료
    if 'local_connection' in locals() and local_connection.is_connected():
        local_cursor.close()
        local_connection.close()
        logger.info("로컬 데이터베이스 연결 종료")
    
    if 'ec2_connection' in locals() and ec2_connection.is_connected():
        ec2_cursor.close()
        ec2_connection.close()
        logger.info("EC2 데이터베이스 연결 종료")

if __name__ == "__main__":
    main() 