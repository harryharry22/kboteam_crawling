# app.py
import pandas as pd
import requests
from datetime import date
from sqlalchemy import create_engine
import os
from flask import Flask, jsonify
from dotenv import load_dotenv
import datetime # datetime 모듈 임포트

# .env 파일 로드 (로컬 테스트용)
load_dotenv()

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# MySQL 연결 정보 설정
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv(
    'DB_URI',
    'mysql+pymysql://root:dugout2025!!@dugout-dev.cn6mm486utfi.ap-northeast-2.rds.amazonaws.com:3306/dugoutDB?charset=utf8'
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

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
                print("Warning: '팀명' 컬럼을 찾을 수 없어 'team_idx' 컬럼이 추가되지 않습니다.")

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

            # '순위' 컬럼은 KBO 웹사이트의 순위이며, team_idx가 고정된 인덱스이므로 '순위'는 필요에 따라 제외할 수 있습니다.
            # 여기서는 요청에 따라 포함하되, team_record_idx는 DB에서 auto-increment될 것이므로 제외합니다.
            ordered_cols = ['순위', 'team_idx', '팀명', 'date', 
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

def save_df_to_db(df: pd.DataFrame):
    """
    DataFrame을 MySQL 데이터베이스의 team_record 테이블에 추가합니다.
    """
    if df is None or df.empty:
        print("저장할 데이터가 없습니다.")
        return False

    db_uri = app.config['SQLALCHEMY_DATABASE_URI']
    if not db_uri:
        print("DB_URI 설정이 누락되었습니다.")
        return False

    try:
        engine = create_engine(db_uri)
        df.to_sql('team_record', con=engine, if_exists='append', index=False)
        print(f"✅ {len(df)}개의 KBO 팀 기록이 'team_record' 테이블에 성공적으로 추가되었습니다.")
        return True
    except Exception as e:
        print(f"❌ 데이터베이스 저장 중 오류 발생: {e}")
        return False

# --- API 엔드포인트 정의 ---
@app.route('/crawl_and_save_kbo_records', methods=['GET'])
def crawl_and_save_kbo_records():
    """
    KBO 팀 기록을 크롤링하여 데이터베이스에 저장하는 API 엔드포인트.
    """
    print(f"[{datetime.datetime.now()}] KBO 팀 기록 크롤링 및 DB 저장 요청 수신...")
    kbo_df = get_kbo_standings_renamed()
    
    if kbo_df is not None:
        success = save_df_to_db(kbo_df)
        if success:
            return jsonify({
                'message': 'KBO 팀 기록이 성공적으로 크롤링되어 데이터베이스에 저장되었습니다.',
                'records_added': len(kbo_df),
                'date': date.today().isoformat()
            }), 200
        else:
            return jsonify({'error': '데이터베이스 저장에 실패했습니다.'}), 500
    else:
        return jsonify({'error': 'KBO 팀 기록을 크롤링하지 못했습니다.'}), 500

@app.route('/')
def home():
    return "KBO 팀 기록 크롤링 및 DB 저장 서비스입니다. '/crawl_and_save_kbo_records' 엔드포인트를 호출하세요."

if __name__ == '__main__':
    # 이 블록은 gunicorn 사용 시 실행되지 않지만, 로컬 개발을 위해 남겨둘 수 있습니다.
    app.run(host='0.0.0.0', port=os.getenv('PORT', 5000), debug=True)
