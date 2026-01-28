"""
실시간 호가 데이터 모델
"""

from datetime import datetime
from pydantic import BaseModel


class AskingPriceMessage(BaseModel):
    """실시간 호가 데이터 메시지 (H0STASP0)"""

    timestamp: datetime

    # 기본 정보
    stock_code: str           # MKSC_SHRN_ISCD: 종목코드
    business_hour: str        # BSOP_HOUR: 영업시간
    hour_cls_code: str        # HOUR_CLS_CODE: 시간구분코드

    # 매도호가 1~10
    askp1: str
    askp2: str
    askp3: str
    askp4: str
    askp5: str
    askp6: str
    askp7: str
    askp8: str
    askp9: str
    askp10: str

    # 매수호가 1~10
    bidp1: str
    bidp2: str
    bidp3: str
    bidp4: str
    bidp5: str
    bidp6: str
    bidp7: str
    bidp8: str
    bidp9: str
    bidp10: str

    # 매도호가잔량 1~10
    askp_rsqn1: str
    askp_rsqn2: str
    askp_rsqn3: str
    askp_rsqn4: str
    askp_rsqn5: str
    askp_rsqn6: str
    askp_rsqn7: str
    askp_rsqn8: str
    askp_rsqn9: str
    askp_rsqn10: str

    # 매수호가잔량 1~10
    bidp_rsqn1: str
    bidp_rsqn2: str
    bidp_rsqn3: str
    bidp_rsqn4: str
    bidp_rsqn5: str
    bidp_rsqn6: str
    bidp_rsqn7: str
    bidp_rsqn8: str
    bidp_rsqn9: str
    bidp_rsqn10: str

    # 총 잔량
    total_askp_rsqn: str      # 총매도호가잔량
    total_bidp_rsqn: str      # 총매수호가잔량
    ovtm_total_askp_rsqn: str # 시간외총매도호가잔량
    ovtm_total_bidp_rsqn: str # 시간외총매수호가잔량

    # 예상체결 정보
    antc_cnpr: str            # 예상체결가
    antc_cnqn: str            # 예상체결량
    antc_vol: str             # 예상체결대금
    antc_cntg_vrss: str       # 예상체결전일대비
    antc_cntg_vrss_sign: str  # 예상체결전일대비부호
    antc_cntg_prdy_ctrt: str  # 예상체결전일대비율

    # 기타
    acml_vol: str             # 누적거래량
    total_askp_rsqn_icdc: str # 총매도호가잔량증감
    total_bidp_rsqn_icdc: str # 총매수호가잔량증감
    ovtm_total_askp_icdc: str # 시간외총매도호가잔량증감
    ovtm_total_bidp_icdc: str # 시간외총매수호가잔량증감
    stck_deal_cls_code: str   # 주식거래구분코드

    @classmethod
    def from_parsed_data(cls, parsed_data: dict) -> "AskingPriceMessage":
        """파싱된 데이터로부터 AskingPriceMessage 생성"""
        return cls(
            timestamp=datetime.now(),
            stock_code=parsed_data.get("MKSC_SHRN_ISCD", ""),
            business_hour=parsed_data.get("BSOP_HOUR", ""),
            hour_cls_code=parsed_data.get("HOUR_CLS_CODE", ""),
            # 매도호가
            askp1=parsed_data.get("ASKP1", ""),
            askp2=parsed_data.get("ASKP2", ""),
            askp3=parsed_data.get("ASKP3", ""),
            askp4=parsed_data.get("ASKP4", ""),
            askp5=parsed_data.get("ASKP5", ""),
            askp6=parsed_data.get("ASKP6", ""),
            askp7=parsed_data.get("ASKP7", ""),
            askp8=parsed_data.get("ASKP8", ""),
            askp9=parsed_data.get("ASKP9", ""),
            askp10=parsed_data.get("ASKP10", ""),
            # 매수호가
            bidp1=parsed_data.get("BIDP1", ""),
            bidp2=parsed_data.get("BIDP2", ""),
            bidp3=parsed_data.get("BIDP3", ""),
            bidp4=parsed_data.get("BIDP4", ""),
            bidp5=parsed_data.get("BIDP5", ""),
            bidp6=parsed_data.get("BIDP6", ""),
            bidp7=parsed_data.get("BIDP7", ""),
            bidp8=parsed_data.get("BIDP8", ""),
            bidp9=parsed_data.get("BIDP9", ""),
            bidp10=parsed_data.get("BIDP10", ""),
            # 매도호가잔량
            askp_rsqn1=parsed_data.get("ASKP_RSQN1", ""),
            askp_rsqn2=parsed_data.get("ASKP_RSQN2", ""),
            askp_rsqn3=parsed_data.get("ASKP_RSQN3", ""),
            askp_rsqn4=parsed_data.get("ASKP_RSQN4", ""),
            askp_rsqn5=parsed_data.get("ASKP_RSQN5", ""),
            askp_rsqn6=parsed_data.get("ASKP_RSQN6", ""),
            askp_rsqn7=parsed_data.get("ASKP_RSQN7", ""),
            askp_rsqn8=parsed_data.get("ASKP_RSQN8", ""),
            askp_rsqn9=parsed_data.get("ASKP_RSQN9", ""),
            askp_rsqn10=parsed_data.get("ASKP_RSQN10", ""),
            # 매수호가잔량
            bidp_rsqn1=parsed_data.get("BIDP_RSQN1", ""),
            bidp_rsqn2=parsed_data.get("BIDP_RSQN2", ""),
            bidp_rsqn3=parsed_data.get("BIDP_RSQN3", ""),
            bidp_rsqn4=parsed_data.get("BIDP_RSQN4", ""),
            bidp_rsqn5=parsed_data.get("BIDP_RSQN5", ""),
            bidp_rsqn6=parsed_data.get("BIDP_RSQN6", ""),
            bidp_rsqn7=parsed_data.get("BIDP_RSQN7", ""),
            bidp_rsqn8=parsed_data.get("BIDP_RSQN8", ""),
            bidp_rsqn9=parsed_data.get("BIDP_RSQN9", ""),
            bidp_rsqn10=parsed_data.get("BIDP_RSQN10", ""),
            # 총 잔량
            total_askp_rsqn=parsed_data.get("TOTAL_ASKP_RSQN", ""),
            total_bidp_rsqn=parsed_data.get("TOTAL_BIDP_RSQN", ""),
            ovtm_total_askp_rsqn=parsed_data.get("OVTM_TOTAL_ASKP_RSQN", ""),
            ovtm_total_bidp_rsqn=parsed_data.get("OVTM_TOTAL_BIDP_RSQN", ""),
            # 예상체결
            antc_cnpr=parsed_data.get("ANTC_CNPR", ""),
            antc_cnqn=parsed_data.get("ANTC_CNQN", ""),
            antc_vol=parsed_data.get("ANTC_VOL", ""),
            antc_cntg_vrss=parsed_data.get("ANTC_CNTG_VRSS", ""),
            antc_cntg_vrss_sign=parsed_data.get("ANTC_CNTG_VRSS_SIGN", ""),
            antc_cntg_prdy_ctrt=parsed_data.get("ANTC_CNTG_PRDY_CTRT", ""),
            # 기타
            acml_vol=parsed_data.get("ACML_VOL", ""),
            total_askp_rsqn_icdc=parsed_data.get("TOTAL_ASKP_RSQN_ICDC", ""),
            total_bidp_rsqn_icdc=parsed_data.get("TOTAL_BIDP_RSQN_ICDC", ""),
            ovtm_total_askp_icdc=parsed_data.get("OVTM_TOTAL_ASKP_ICDC", ""),
            ovtm_total_bidp_icdc=parsed_data.get("OVTM_TOTAL_BIDP_ICDC", ""),
            stck_deal_cls_code=parsed_data.get("STCK_DEAL_CLS_CODE", ""),
        )
