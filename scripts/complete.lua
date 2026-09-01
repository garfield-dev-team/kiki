-- complete.lua — 转移 #6（幂等 + 幽灵写防护）
--
-- KEYS[1]=lease  KEYS[2]=task hash
-- ARGV[1]=worker  ARGV[2]=token  ARGV[3]=task_id  ARGV[4]=result  ARGV[5]=retention_ms
--
-- 返回: {OK} | {OK_DUP} | {ERR_GONE} | {ERR_STATE[, st]} | {ERR_FENCED}
--
-- 勘误 #2（go-implementation.md §3.5）：task_id 由 ARGV 显式传入，
-- 直接 ZREM lease；design.md §5.4 示意伪码无效。
--
-- 故意不检查租约是否已过期，只检查 owner+token：租约易主只能经 sweep 发生，
-- sweep 必然 ver+1；token 仍匹配 ⇒ sweep 尚未触碰此任务 ⇒ 此刻无人可能持有它。
-- 与 sweep 在同一脚本域内线性化，先到先赢（design.md §5.4）。不要加过期拒绝逻辑。

local tk = KEYS[2]
if redis.call('EXISTS', tk) == 0 then return {'ERR_GONE'} end
local st = redis.call('HGET', tk, 'state')
local same = redis.call('HGET', tk, 'owner') == ARGV[1]
             and tonumber(redis.call('HGET', tk, 'ver')) == tonumber(ARGV[2])
if st == 'COMPLETED' and same then return {'OK_DUP'} end   -- 响应丢失后的重试
if st ~= 'RESERVED' then return {'ERR_STATE', st} end
if not same then return {'ERR_FENCED'} end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('HSET', tk, 'state', 'COMPLETED', 'result', ARGV[4],
           'finished_ms', now, 'updated_ms', now)
redis.call('ZREM', KEYS[1], ARGV[3])
redis.call('EXPIRE', tk, tonumber(ARGV[5]))
return {'OK'}
