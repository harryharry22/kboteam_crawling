# update_team_records.py
import pandas as pd
import requests
from datetime import date # For fallback in case date extraction fails
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

from bs4 import BeautifulSoup # Added
import re # Added

# .env 파일 로드 (로컬 테스트용, GitHub Actions에서는 secrets로 관리)
load_dotenv()

def get_kbo_standings_renamed():
    """
    KBO 일일 팀 순위를 크롤링하여 팀 인덱스, 크롤링 날짜를 추가하고,
    컬럼명을 변경하여 pandas DataFrame으로 반환합니다.
    'date' 컬럼은 웹페이지의 내용에서 추출됩니다.
    """
    url = "https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx"
    
    try:
        html_content = requests.get(url).content
        
        # --- Extract date from HTML content ---
        soup = BeautifulSoup(html_content, 'html.parser')
        date_span = soup.find('span', class_='exp2')
        extracted_date_str = None
        if date_span:
            date_text = date_span.get_text()
            # Expected format: (YYYY년 MM월DD일 기준)
            match = re.search(r'\((\d{4})년 (\d{2})월(\d{2})일 기준\)', date_text)
            if match:
                year = match.group(1)
                month = match.group(2)
                day = match.group(3)
                extracted_date_str = f"{year}-{month}-{day}"
            else:
                print(f"Warning: Could not parse date from text: {date_text}. Using today's date.")
                extracted_date_str = date.today().isoformat() # Fallback to today's date
        else:
            print("Warning: Date span with class 'exp2' not found on the page. Using today's date.")
            extracted_date_str = date.today().isoformat() # Fallback to today's date
        
        # --- Continue with DataFrame creation ---
        df_list = pd.read_html(html_content)
        
        if df_list:
            df = df_list[0]
            
            # Define the team index mapping
            team_mapping = {
                'LG': 1, 'SSG': 2, '삼성': 3, 'KT': 4, '롯데': 5,
                'NC': 6, '두산': 7, '키움': 8, 'KIA': 9, '한화': 10
            }
            
            # Create the 'team_idx' column using the mapping
            if '팀명' in df.columns:
                df['team_idx'] = df['팀명'].map(team_mapping)
            else:
                print("Warning: '팀명' column not found, 'team_idx' column will not be added.")

            # Add the 'date' column with the extracted date
            df['date'] = extracted_date_str
            
            # Define the new column names
            new_column_names = {
                '경기': 'game',
                '승': 'win',
                '패': 'lose',
                '무': 'draw',
                '승률': 'win_rate',
                '게임차': 'game_gap',
                '최근10경기': 'recent_ten',
                '연속': 'streak',
                '홈': 'home_record',
                '방문': 'away_record'
            }
            
            # Rename the columns
            df = df.rename(columns=new_column_names)

            # Define the desired order of columns (Removed '순위' and '팀명' for a cleaner DB record based on common practice)
            ordered_cols = ['team_idx', 'date', 
                            'game', 'win', 'lose', 'draw', 'win_rate', 'game_gap',
                            'recent_ten', 'streak', 'home_record', 'away_record']
            
            # Filter ordered_cols to only include columns that actually exist in the DataFrame
            final_ordered_cols = [col for col in ordered_cols if col in df.columns]
            
            df = df[final_ordered_cols]
            
            return df
        else:
            print("No tables found on the page.")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def save_to_db(df: pd.DataFrame):
    """
    DataFrame을 MySQL 데이터베이스의 team_record 테이블에 추가합니다.
    """
    if df is None or df.empty:
        print("저장할 데이터가 없습니다.")
        return False

    # DB 연결 정보 가져오기 (환경 변수 또는 GitHub Actions secrets)
    db_uri = os.getenv('DB_URI')
    if not db_uri:
        print("DB_URI 환경 변수가 설정되지 않았습니다.")
        return False

    try:
        # SQLAlchemy 엔진 생성
        engine = create_engine(db_uri)

        # DataFrame을 'team_record' 테이블에 추가 (append 모드)
        # index=False: DataFrame의 인덱스를 DB 컬럼으로 저장하지 않습니다.
        # if_exists='append': 테이블이 존재하면 데이터만 추가합니다.
        # team_record_idx는 DB에서 auto-increment되므로 DataFrame에 추가하지 않아도 됩니다.
        df.to_sql('team_record', con=engine, if_exists='append', index=False)
        print(f"✅ {len(df)}개의 KBO 팀 기록이 'team_record' 테이블에 성공적으로 추가되었습니다.")
        return True
    except Exception as e:
        print(f"❌ 데이터베이스 저장 중 오류 발생: {e}")
        return False

if __name__ == "__main__":
    print("KBO 팀 기록 크롤링 및 DB 저장 스크립트 시작...")
    kbo_df = get_kbo_standings_renamed()
    
    if kbo_df is not None:
        print("크롤링된 데이터 (상위 5개 행):")
        print(kbo_df.head())
        save_to_db(kbo_df)
    else:
        print("KBO 팀 기록을 가져오지 못했습니다.")
