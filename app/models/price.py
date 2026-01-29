"""
실시간 가격 데이터 모델
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, field_validator


class PriceMessage(BaseModel):
    """실시간 가격 데이터 메시지"""

    timestamp: datetime
    stock_code: str  # MKSC_SHRN_ISCD
    trade_time: str  # STCK_CNTG_HOUR
    current_price: str  # STCK_PRPR
    price_change_sign: str  # PRDY_VRSS_SIGN
    price_change: str  # PRDY_VRSS
    price_change_rate: str  # PRDY_CTRT
    weighted_avg_price: str  # WGHN_AVRG_STCK_PRC
    open_price: str  # STCK_OPRC
    high_price: str  # STCK_HGPR
    low_price: str  # STCK_LWPR
    ask_price1: str  # ASKP1
    bid_price1: str  # BIDP1
    trade_volume: str  # CNTG_VOL
    accumulated_volume: str  # ACML_VOL
    accumulated_trade_amount: str  # ACML_TR_PBMN
    sell_trade_count: str  # SELN_CNTG_CSNU
    buy_trade_count: str  # SHNU_CNTG_CSNU
    net_buy_trade_count: str  # NTBY_CNTG_CSNU
    trade_strength: str  # CTTR
    total_sell_trade_volume: str  # SELN_CNTG_SMTN
    total_buy_trade_volume: str  # SHNU_CNTG_SMTN
    trade_type: str  # CNTG_CLS_CODE
    buy_rate: str  # SHNU_RATE
    volume_ratio: str  # PRDY_VOL_VRSS_ACML_VOL_RATE
    open_time: str  # OPRC_HOUR
    open_price_change_sign: str  # OPRC_VRSS_PRPR_SIGN
    open_price_change: str  # OPRC_VRSS_PRPR
    high_time: str  # HGPR_HOUR
    high_price_change_sign: str  # HGPR_VRSS_PRPR_SIGN
    high_price_change: str  # HGPR_VRSS_PRPR
    low_time: str  # LWPR_HOUR
    low_price_change_sign: str  # LWPR_VRSS_PRPR_SIGN
    low_price_change: str  # LWPR_VRSS_PRPR
    business_date: str  # BSOP_DATE
    new_market_open_code: str  # NEW_MKOP_CLS_CODE
    trading_halt_yn: str  # TRHT_YN
    ask_remaining1: str  # ASKP_RSQN1
    bid_remaining1: str  # BIDP_RSQN1
    total_ask_remaining: str  # TOTAL_ASKP_RSQN
    total_bid_remaining: str  # TOTAL_BIDP_RSQN
    volume_turnover_rate: str  # VOL_TNRT
    prev_same_time_volume: str  # PRDY_SMNS_HOUR_ACML_VOL
    prev_same_time_volume_rate: str  # PRDY_SMNS_HOUR_ACML_VOL_RATE
    time_class_code: str  # HOUR_CLS_CODE
    market_trade_class_code: str  # MRKT_TRTM_CLS_CODE
    vi_standard_price: str  # VI_STND_PRC

    @classmethod
    def from_parsed_data(cls, parsed_data: dict) -> "PriceMessage":
        """파싱된 데이터로부터 PriceMessage 생성"""
        return cls(
            timestamp=datetime.now(),
            stock_code=parsed_data.get("MKSC_SHRN_ISCD", ""),
            trade_time=parsed_data.get("STCK_CNTG_HOUR", ""),
            current_price=parsed_data.get("STCK_PRPR", ""),
            price_change_sign=parsed_data.get("PRDY_VRSS_SIGN", ""),
            price_change=parsed_data.get("PRDY_VRSS", ""),
            price_change_rate=parsed_data.get("PRDY_CTRT", ""),
            weighted_avg_price=parsed_data.get("WGHN_AVRG_STCK_PRC", ""),
            open_price=parsed_data.get("STCK_OPRC", ""),
            high_price=parsed_data.get("STCK_HGPR", ""),
            low_price=parsed_data.get("STCK_LWPR", ""),
            ask_price1=parsed_data.get("ASKP1", ""),
            bid_price1=parsed_data.get("BIDP1", ""),
            trade_volume=parsed_data.get("CNTG_VOL", ""),
            accumulated_volume=parsed_data.get("ACML_VOL", ""),
            accumulated_trade_amount=parsed_data.get("ACML_TR_PBMN", ""),
            sell_trade_count=parsed_data.get("SELN_CNTG_CSNU", ""),
            buy_trade_count=parsed_data.get("SHNU_CNTG_CSNU", ""),
            net_buy_trade_count=parsed_data.get("NTBY_CNTG_CSNU", ""),
            trade_strength=parsed_data.get("CTTR", ""),
            total_sell_trade_volume=parsed_data.get("SELN_CNTG_SMTN", ""),
            total_buy_trade_volume=parsed_data.get("SHNU_CNTG_SMTN", ""),
            trade_type=parsed_data.get("CNTG_CLS_CODE", ""),
            buy_rate=parsed_data.get("SHNU_RATE", ""),
            volume_ratio=parsed_data.get("PRDY_VOL_VRSS_ACML_VOL_RATE", ""),
            open_time=parsed_data.get("OPRC_HOUR", ""),
            open_price_change_sign=parsed_data.get("OPRC_VRSS_PRPR_SIGN", ""),
            open_price_change=parsed_data.get("OPRC_VRSS_PRPR", ""),
            high_time=parsed_data.get("HGPR_HOUR", ""),
            high_price_change_sign=parsed_data.get("HGPR_VRSS_PRPR_SIGN", ""),
            high_price_change=parsed_data.get("HGPR_VRSS_PRPR", ""),
            low_time=parsed_data.get("LWPR_HOUR", ""),
            low_price_change_sign=parsed_data.get("LWPR_VRSS_PRPR_SIGN", ""),
            low_price_change=parsed_data.get("LWPR_VRSS_PRPR", ""),
            business_date=parsed_data.get("BSOP_DATE", ""),
            new_market_open_code=parsed_data.get("NEW_MKOP_CLS_CODE", ""),
            trading_halt_yn=parsed_data.get("TRHT_YN", ""),
            ask_remaining1=parsed_data.get("ASKP_RSQN1", ""),
            bid_remaining1=parsed_data.get("BIDP_RSQN1", ""),
            total_ask_remaining=parsed_data.get("TOTAL_ASKP_RSQN", ""),
            total_bid_remaining=parsed_data.get("TOTAL_BIDP_RSQN", ""),
            volume_turnover_rate=parsed_data.get("VOL_TNRT", ""),
            prev_same_time_volume=parsed_data.get("PRDY_SMNS_HOUR_ACML_VOL", ""),
            prev_same_time_volume_rate=parsed_data.get("PRDY_SMNS_HOUR_ACML_VOL_RATE", ""),
            time_class_code=parsed_data.get("HOUR_CLS_CODE", ""),
            market_trade_class_code=parsed_data.get("MRKT_TRTM_CLS_CODE", ""),
            vi_standard_price=parsed_data.get("VI_STND_PRC", ""),
        )
