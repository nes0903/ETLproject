#!/bin/bash

# 작업 디렉토리로 이동
cd /home/ubuntu/ETLproject

# logs 디렉토리 생성
mkdir -p logs

# 타임스탬프 생성
timestamp=$(date +"%Y%m%d_%H%M%S")

# 로그 파일 생성
log_file="logs/etl_process_${timestamp}.log"

# ETL 프로세스 실행
echo "ETL 프로세스 시작: $(date)" | tee -a "$log_file"
python3 local_to_ec2_etl.py 2>&1 | tee -a "$log_file"
etl_status=$?

# ETL 프로세스 성공 여부 확인
if [ $etl_status -eq 0 ]; then
    echo "ETL 프로세스 성공: $(date)" | tee -a "$log_file"
    
    # 검증 프로세스 실행
    echo "검증 프로세스 시작: $(date)" | tee -a "$log_file"
    python3 data_validation.py 2>&1 | tee -a "$log_file"
    validation_status=$?
    
    # 검증 프로세스 성공 여부 확인
    if [ $validation_status -eq 0 ]; then
        echo "검증 프로세스 성공: $(date)" | tee -a "$log_file"
    else
        echo "검증 프로세스 실패: $(date)" | tee -a "$log_file"
    fi
else
    echo "ETL 프로세스 실패: $(date)" | tee -a "$log_file"
fi

# 30일 이상 된 로그 파일 삭제
find logs -name "etl_process_*.log" -type f -mtime +30 -delete
find logs -name "validation_*.log" -type f -mtime +30 -delete 