import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
import requests
import io
import json
from datetime import datetime
import numpy as np
import logging

# 로그 설정
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, f"local_to_ec2_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 시작 시간 기록
start_time = datetime.now()
logger.info("ETL 프로세스 시작")

# 처리할 Excel 파일 목록
EXCEL_FILES = {
    '위해상품': 'https://raw.githubusercontent.com/nes0903/ETLproject/main/위해상품판매차단/위해상품.xlsx',
    '위해상품부적합검사': 'https://raw.githubusercontent.com/nes0903/ETLproject/main/위해상품판매차단/위해상품부적합검사.xlsx',
    '위해상품업체': 'https://raw.githubusercontent.com/nes0903/ETLproject/main/위해상품판매차단/위해상품업체.xlsx'
}

# 데이터베이스 연결 설정
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'kopo',
    'password': '1234',
    'database': 'kopo'
}

EC2_DB_CONFIG = {
    'host': '54.180.237.238',  # EC2 인스턴스의 퍼블릭 IP
    'port': 3306,
    'user': 'kopo',
    'password': '1234',
    'database': 'kopo2'
}

try:
    # 1. 데이터베이스 연결
    # 로컬 kopo 데이터베이스 연결
    local_connection = mysql.connector.connect(**LOCAL_DB_CONFIG)
    local_cursor = local_connection.cursor()
    logger.info("로컬 kopo 데이터베이스 연결 성공")
    
    # EC2 kopo2 데이터베이스 연결
    ec2_connection = mysql.connector.connect(**EC2_DB_CONFIG)
    ec2_cursor = ec2_connection.cursor()
    logger.info("EC2 kopo2 데이터베이스 연결 성공")
    
    # 2. 이력 테이블 생성 (EC2 DB에)
    create_history_table_query = """
    CREATE TABLE IF NOT EXISTS 데이터셋_최신화_이력 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        process_name VARCHAR(255),
        status VARCHAR(50),
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    ec2_cursor.execute(create_history_table_query)
    ec2_connection.commit()
    logger.info("데이터셋_최신화_이력 테이블 생성 완료")
    
    # 3. Excel 파일 처리
    for table_name, url in EXCEL_FILES.items():
        logger.info(f"\n처리 중인 파일: {table_name}")
        
        # 파일 다운로드
        logger.info(f"다운로드 시도: {url}")
        response = requests.get(url)
        response.raise_for_status()
        logger.info(f"다운로드 성공: {table_name}")
        file_content = io.BytesIO(response.content)
        
        # Excel 데이터 읽기
        df = pd.read_excel(file_content)
        
        # 테이블 생성 (로컬 DB에)
        type_mapping = {
            'object': 'VARCHAR(255)',
            'int64': 'INT',
            'float64': 'FLOAT',
            'datetime64[ns]': 'DATETIME',
            'bool': 'BOOLEAN'
        }
        
        columns = []
        for column, dtype in df.dtypes.items():
            mysql_type = type_mapping.get(str(dtype), 'VARCHAR(255)')
            columns.append(f"`{column}` {mysql_type}")
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
        """
        
        local_cursor.execute(create_table_query)
        logger.info(f"로컬 DB에 테이블 {table_name} 생성 완료")
        
        # 데이터 삽입 (로컬 DB에)
        df = df.replace({pd.NA: None})
        df = df.replace({np.nan: None})
        
        columns = [f"`{col}`" for col in df.columns]
        insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
        """
        
        values = df.values.tolist()
        local_cursor.executemany(insert_query, values)
        local_connection.commit()
        logger.info(f"로컬 DB 테이블 {table_name}에 데이터 삽입 완료")
    
    # 4. 통합 테이블 생성 (EC2 DB에)
    create_joined_table_query = """
    CREATE TABLE IF NOT EXISTS 위해상품_위험도_분석 AS
    SELECT 
        w.doc_no,
        w.doc_cycl,
        w.prdct_nm as 제품명,
        w.prdct_type_nm as 제품유형,
        w.plor_nm as 제조국,
        w.mnftr_ymd as 제조일자,
        w.rtl_term_cn as 유통기한,
        w.safe_cert_no as 안전인증번호,
        w.prdct_prmsn_no as 제품허가번호,
        w.rtrvl_rsn_nm as 회수조치내용,
        w.rtrvl_rsn_cd as 회수조치코드,
        w.rpt_ymd as 신고일자,
        w.ntfctn_dt as 통보일자,
        w.cmd_bgng_dd_cn as 회수시작일,
        b.icpt_insp_artcl_cn as 부적합검사항목,
        b.icpt_insp_spcfct_cn as 부적합검사규격,
        b.icpt_insp_rslt_cn as 부적합검사결과,
        u.bzenty_type_nm as 업체유형,
        u.bzenty_nm as 업체명,
        u.bzenty_brno as 사업자등록번호,
        u.bzenty_addr as 업체주소,
        CASE 
            WHEN w.rtrvl_rsn_cd IS NOT NULL THEN 1
            ELSE 0
        END as 위해상품여부,
        CASE 
            WHEN w.rtrvl_rsn_cd LIKE '%1%' THEN '위험'
            WHEN w.rtrvl_rsn_cd LIKE '%2%' THEN '주의'
            WHEN w.rtrvl_rsn_cd LIKE '%3%' THEN '관심'
            ELSE '정상'
        END as 위험도등급
    FROM kopo.위해상품 w
    LEFT JOIN kopo.위해상품부적합검사 b 
        ON w.doc_no = b.doc_no 
        AND w.doc_cycl = b.doc_cycl
    LEFT JOIN kopo.위해상품업체 u 
        ON w.doc_no = u.doc_no 
        AND w.doc_cycl = u.doc_cycl
    WHERE w.prdct_nm IS NOT NULL
        AND w.prdct_type_nm IS NOT NULL
        AND w.plor_nm IS NOT NULL
    """
    
    ec2_cursor.execute(create_joined_table_query)
    logger.info("EC2 DB에 위해상품_위험도_분석 테이블 생성 완료")
    
    # 5. 데이터셋 테이블 생성 (EC2 DB에)
    create_dataset_table_query = """
    CREATE TABLE IF NOT EXISTS 위해상품_데이터셋 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        input_data JSON,
        output_text TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """
    ec2_cursor.execute(create_dataset_table_query)
    logger.info("EC2 DB에 데이터셋 테이블 생성 완료")
    
    # 6. 데이터셋 생성 (EC2 DB에)
    ec2_cursor.execute("""
        SELECT DISTINCT
            제품명,
            제조국,
            부적합검사항목,
            부적합검사결과,
            회수조치내용,
            위험도등급,
            제품유형,
            제조일자,
            유통기한,
            업체유형
        FROM 위해상품_위험도_분석
        WHERE 부적합검사결과 IS NOT NULL
        AND 제품명 IS NOT NULL
        AND 제조국 IS NOT NULL
    """)
    rows = ec2_cursor.fetchall()
    
    insert_query = """
    INSERT INTO 위해상품_데이터셋 (input_data, output_text)
    VALUES (%s, %s)
    """
    
    for row in rows:
        # 입력 데이터 구성
        input_data = {
            "제품명": row[0],
            "제조국": row[1],
            "제품유형": row[6],
            "제조일자": row[7],
            "유통기한": row[8],
            "검사결과": row[3],
            "업체유형": row[9],
            "위험도등급": row[5]
        }
        
        # 회수 사유 생성
        item = row[2] if row[2] else ""
        result = row[3] if row[3] else ""
        
        if not item or not result:
            output_text = row[4] if row[4] else "검사결과 부적합"
        elif "대장균" in item or "황색포도상구균" in item or "세균" in item or "리스테리아" in item:
            output_text = f"미생물 오염({item}) 기준치 초과로 인한 회수"
        elif "중금속" in item or "납" in item or "카드뮴" in item:
            output_text = f"중금속({item}) 기준치 초과로 인한 회수"
        elif "보존료" in item or "합성" in item:
            if "검출" in result:
                detected = result.split("검출")[0].strip()
                output_text = f"합성보존료({detected}) 검출로 인한 회수"
            else:
                output_text = f"합성보존료 기준치 초과로 인한 회수"
        elif "이물" in item:
            output_text = "이물질 혼입으로 인한 회수"
        else:
            output_text = f"{item} 기준 부적합으로 인한 회수"
        
        # 데이터 저장
        ec2_cursor.execute(insert_query, (json.dumps(input_data, ensure_ascii=False), output_text))
    
    ec2_connection.commit()
    logger.info(f"총 {len(rows)}개의 데이터셋 생성 완료")
    
    # 7. 성공 이력 기록 (EC2 DB에)
    insert_history_query = """
        INSERT INTO 데이터셋_최신화_이력 
        (process_name, status, error_message)
        VALUES (%s, %s, %s)
    """
    ec2_cursor.execute(insert_history_query, ("ETL 프로세스", "성공", None))
    ec2_connection.commit()
    logger.info("업데이트 이력 기록 완료")
    
except Exception as e:
    # 실패 이력 기록 (EC2 DB에)
    try:
        insert_history_query = """
            INSERT INTO 데이터셋_최신화_이력 
            (process_name, status, error_message)
            VALUES (%s, %s, %s)
        """
        ec2_cursor.execute(insert_history_query, ("ETL 프로세스", "실패", str(e)))
        ec2_connection.commit()
    except:
        pass
    logger.error(f"프로세스 실행 중 오류 발생: {str(e)}")
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