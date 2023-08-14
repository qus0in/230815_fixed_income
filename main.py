from dotenv import load_dotenv
import os
import requests as 클라이언트
import pandas as pd
import streamlit as st

def 채권시세정보데이터요청(기준일: str):
    URL = 'http://apis.data.go.kr/1160100/service/GetBondSecuritiesInfoService/getBondPriceInfo'
    접속키 = os.getenv('ACCESS_KEY')
    params = dict(
        serviceKey=접속키,
        resultType='json',
        basDt=기준일,
        numOfRows=1000,
    )
    응답 = 클라이언트.get(URL, params=params)
    df = pd.DataFrame(응답.json()['response']['body']['items']['item'])
    columnMapper = dict(
        isinCd='코드',
        itmsNm='채권명',
        trqu='거래량',
        clprPrc='가격',
        clprBnfRt='수익률',
    )
    df.rename(columns=columnMapper, inplace=True)
    df = df.loc[:, ['코드', '채권명', '거래량', '가격', '수익률']]
    df.set_index('코드', inplace=True)
    df.to_csv(f'채권시세정보_{기준일}.csv')
    return df

def 채권시세정보가져오기(기준일: str):
    df: pd.DataFrame
    try:
        df = pd.read_csv(f'채권시세정보_{기준일}.csv', index_col='코드')
    except:
        df = 채권시세정보데이터요청(기준일)
    return df

def 채권기본정보데이터요청(기준일: str):
    URL = 'http://apis.data.go.kr/1160100/service/GetBondIssuInfoService/getBondBasiInfo'
    접속키 = os.getenv('ACCESS_KEY')
    params = dict(
        serviceKey=접속키,
        resultType='json',
        basDt=기준일,
        numOfRows=30000,
    )
    응답 = 클라이언트.get(URL, params=params)
    df = pd.DataFrame(응답.json()['response']['body']['items']['item'])
    columnMapper = dict(
        isinCd='코드',
        # isinCdNm='채권명',
        scrsItmsKcdNm='채권분류',
        bondIsurNm='채권발행인명',
        sicNm='표준산업분류명',
        bondExprDt='채권만기일자',
        bondSrfcInrt='채권표면이율',
        bondRnknDcdNm='채권순위구분',
        bondIntTcdNm='채권이자유형',
        intPayCyclCtt='이자지급주기',
        intPayMmntDcdNm='이자지급시기',
        kisScrsItmsKcdNm='한국신용평가신용도',
        kbpScrsItmsKcdNm='한국자산평가신용도',
        niceScrsItmsKcdNm='NICE평가정보신용도',
    )
    df.rename(columns=columnMapper, inplace=True)
    df = df.loc[:, columnMapper.values()]
    df.set_index('코드', inplace=True)
    df.to_csv(f'채권기본정보_{기준일}.csv')
    return df

def 채권기본정보가져오기(기준일: str):
    df: pd.DataFrame
    try:
        df = pd.read_csv(f'채권기본정보_{기준일}.csv', index_col='코드')
    except:
        df = 채권기본정보데이터요청(기준일)
    return df

def 채권정보가져오기(기준일: str):
    df: pd.DataFrame
    try:
        df = pd.read_csv(f'채권정보_{기준일}.csv', index_col='코드')
    except:
        채권시세정보 = 채권시세정보가져오기(기준일)
        채권기본정보 = 채권기본정보가져오기(기준일)
        채권정보 = 채권시세정보.join(채권기본정보, how='inner').loc[:,
        ['채권명', '거래량', '가격', '수익률', '채권분류', '채권발행인명', '채권만기일자', '채권표면이율', '이자지급주기',
         '한국신용평가신용도', '한국자산평가신용도', 'NICE평가정보신용도']]
        # '채권이자유형', '이자지급시기
        채권정보.to_csv(f'채권정보_{기준일}.csv')
        df = 채권정보.copy()
    return df

if __name__ == '__main__':
    load_dotenv()
    기준일 = '20230811'
    채권정보 = 채권정보가져오기(기준일)
    채권정보.채권만기일자 = pd.to_datetime(
        채권정보.채권만기일자.apply(str), format='%Y%m%d', errors='coerce').dt.date
    채권정보 = 채권정보.sort_values('거래량', ascending=False)
    채권정보['세후수익률'] = 채권정보.수익률 * (1 - 0.154)
    채권정보['신용도'] = 채권정보.apply(lambda x: '/'.join(set([str(y).replace('0', '') for y in (x.한국신용평가신용도, x.한국자산평가신용도, x.NICE평가정보신용도) if str(y) != 'nan'])), axis=1)
    채권정보.rename(columns={'채권발행인명': '채권발행'}, inplace=True)
    st.dataframe(
        채권정보.loc[:,
        ['채권명', '거래량', '세후수익률', '채권표면이율', '채권분류',
         '신용도', '이자지급주기',
        #  '채권발행' : 채권명에 포함
        #  '이자지급시기' : 일반적으로 국채/지방채와 관련
         ]], hide_index=True, use_container_width=True,
    )

'''
테스트용 API를 활용해서 최신 데이터의 현재 날짜를 구한다
streamlit으로 merge해버림, caching도 겸함. -> 가장 간단한 건 supabase를 쓰거나, lambda 등을 써버린다던가...
-> dynamodb에 넣어버리자...
'''