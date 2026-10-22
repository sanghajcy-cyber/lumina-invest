"""월초 ATR 리밸런싱 전략(atr_rebal).

  targets  목표 포트폴리오 파일(CSV/xlsx) 파싱·검증 — 순수 함수
  planner  ATR·투자금액·목표수량 계산, plan_*.csv 형식 행 — 순수 함수
  service  DB·게이트웨이·KIS 시세를 묶는 진입점 (플랜 설정, 업로드, dry-run, 활성화, auto_trade 차단)
"""
