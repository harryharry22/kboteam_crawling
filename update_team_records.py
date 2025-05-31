import pandas as pd
import requests
from datetime import date
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# .env 파일 로드 (로컬 테스트용, GitHub Actions에서는 secrets로 관리)
load_dotenv()

def get_kbo_standings_renamed():
    """
    KBO 일일 팀 순위를 크롤링하여 팀 인덱스, 크롤링 날짜를 추가하고,
    컬럼명을 변경하여 pandas DataFrame으로 반환합니다.
    """
    url = "https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx"
    
    try:
        html_content = requests.get(url).content
        df_list = pd.read_html(html_content)
        
        if df_list:
            df = df_list[0]
            
            # 팀 인덱스 매핑 정의
            team_mapping = {
                'LG': 1, 'SSG': 2, '삼성': 3, 'KT': 4, '롯데': 5,
                'NC': 6, '두산': 7, '키움': 8, 'KIA': 9, '한화': 10
            }
            
            # 'team_idx' 컬럼 생성
            if '팀명' in df.columns:
                df['team_idx'] = df['팀명'].map(team_mapping)
            else:
                print("경고: '팀명' 컬럼을 찾을 수 없어 'team_idx' 컬럼이 추가되지 않습니다.")

            # 'date' 컬럼 추가 (현재 날짜)
            df['date'] = date.today().isoformat() # 날짜를 YYYY-MM-DD 형식의 문자열로
            
            # 새 컬럼명 정의
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
            
            # 컬럼명 변경
            df = df.rename(columns=new_column_names)

            # 필요한 컬럼만 선택하고 순서 재정렬
            # '팀명' 컬럼은 DB에 저장할 때 필요할 수 있으므로 일단 포함합니다.
            # team_record_idx는 DB에서 auto-increment될 것이므로 DataFrame에는 포함하지 않습니다.
            ordered_cols = ['team_idx', '팀명', 'date', 
                            'game', 'win', 'lose', 'draw', 'win_rate', 'game_gap',
                            'recent_ten', 'streak', 'home_record', 'away_record']
            
            df = df[ordered_cols]

            return df
        else:
            print("페이지에서 테이블을 찾을 수 없습니다.")
            return None
    except Exception as e:
        print(f"오류 발생: {e}")
        return None

def save_to_db(df: pd.DataFrame):
    """
    DataFrame을 MySQL 데이터베이스의 team_record 테이블에 추가합니다.
    """
    if df is None or df.empty:
        print("저장할 데이터가 없습니다.")
        return

    # DB 연결 정보 가져오기 (환경 변수 또는 GitHub Actions secrets)
    db_uri = os.getenv('DB_URI')
    if not db_uri:
        print("DB_URI 환경 변수가 설정되지 않았습니다.")
        return

    try:
        # SQLAlchemy 엔진 생성
        engine = create_engine(db_uri)

        # DataFrame을 'team_record' 테이블에 추가 (append 모드)
        # index=False: DataFrame의 인덱스를 DB 컬럼으로 저장하지 않습니다.
        # if_exists='append': 테이블이 존재하면 데이터만 추가합니다.
        # team_record_idx는 DB에서 auto-increment되므로 DataFrame에 추가하지 않아도 됩니다.
        df.to_sql('team_record', con=engine, if_exists='append', index=False)
        print(f"✅ {len(df)}개의 KBO 팀 기록이 'team_record' 테이블에 성공적으로 추가되었습니다.")

    except Exception as e:
        print(f"❌ 데이터베이스 저장 중 오류 발생: {e}")

if __name__ == "__main__":
    print("KBO 팀 기록 크롤링 및 DB 저장 스크립트 시작...")
    kbo_df = get_kbo_standings_renamed()
    
    if kbo_df is not None:
        print("크롤링된 데이터:")
        print(kbo_df.head()) # 상위 5개 행만 출력하여 확인
        save_to_db(kbo_df)
    else:
        print("KBO 팀 기록을 가져오지 못했습니다.")
