"""SS03 玩家分组口径（唯一取数口径，供所有 SS03 分析复用）。

切换点：2026-08-03 16:00 PDT = 2026-08-03 23:00 UTC = 2026-08-04 07:00 北京。
  · 切换后（created_at >= 该 UTC）：按 user_id 尾号 MOD(user_id,10) 静态分组
        Default = 0,1,2,3 (40%) · AB_TEST_A = 4,5 (20%) · AB_TEST_B = 6,7 (20%) · AI = 8,9 (20%)
    静态：每人永远一组，各组人数之和 == 总人数（可用此验证分流）。
  · 切换前：按注单 partition_ab[0]（ab_group_id）映射（分组跟注单走，可跨组）。

表：slot-machine.public.fct_bet_orders。partition_ab 是 SUPER 数组，用 partition_ab[0]::varchar 取首元素。
"""
SS03_SWITCH_UTC = "2026-08-03 23:00:00"   # = 2026-08-03 16:00 PDT

# 旧准则 A/B ↔ ab_group_id 映射（采用「准则」文本表：A=4f1a46ca(95kai)、B=4a04df21(BG97)，与既有更正一致）。
# 注：用户所给 SQL 片段里 A/B 与此相反，属其消息内部不一致；此处以准则文本表为准，待确认。
GROUP_CASE = f"""CASE
  WHEN created_at >= '{SS03_SWITCH_UTC}' THEN
    CASE
      WHEN MOD(user_id, 10) IN (0,1,2,3) THEN 'Default'
      WHEN MOD(user_id, 10) IN (4,5)     THEN 'AB_TEST_A'
      WHEN MOD(user_id, 10) IN (6,7)     THEN 'AB_TEST_B'
      ELSE 'AI'
    END
  WHEN partition_ab[0]::varchar = 'jojpin-9mokha-rexQug'                    THEN 'AI'
  WHEN partition_ab[0]::varchar = '4f1a46ca-7baa-4452-9a40-ef21d9b33b57'   THEN 'AB_TEST_A'
  WHEN partition_ab[0]::varchar = '4a04df21-c749-4808-8e55-3a0b74c084d2'   THEN 'AB_TEST_B'
  ELSE 'Default'
END"""

BASE_FILTER = ("game_id='SS03' AND currency_type='CNY' AND status='COMPLETED' "
               "AND op_code NOT IN ('B26','TST','TSB','TSO')")
