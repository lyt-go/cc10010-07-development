<template>
  <section class="page" data-module="depledger">
    <header class="page-head">
      <div>
        <h2>依赖就绪账本</h2>
        <p class="page-desc">从依赖关系偏移与等待事件计算依赖就绪数据快照，仅在上游水位满足触发条件时生成下游可执行结果；刷新后水位与等待结果保持同一版口径。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="runRecompute">重新解析依赖</button>
        <button class="btn" type="button" @click="exportRows">导出账本</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in statCards" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <div class="tab-row">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="tab-btn"
        :class="{ active: activeTab === tab.key }"
        type="button"
        @click="switchTab(tab.key)"
      >
        {{ tab.label }}
      </button>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>快照编号 / 下游作业</span>
        <input v-model="keyword" placeholder="按快照编号或下游作业检索" />
      </label>
      <label class="filter-item">
        <span>就绪状态</span>
        <select v-model="stateFilter">
          <option value="">全部</option>
          <option v-for="s in ledgerStates" :key="s" :value="s">{{ s }}</option>
        </select>
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table v-if="activeTab === 'ledger'" class="data-table">
      <thead>
        <tr>
          <th v-for="column in ledgerColumns" :key="column">{{ column }}</th>
          <th>核对 / 恢复</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in ledgerRows" :key="String(row.id)">
          <td v-for="column in ledgerColumns" :key="column">
            <span v-if="column === '就绪状态'" class="state-tag" :class="stateClass(row[column])">{{ row[column] }}</span>
            <span v-else>{{ row[column] ?? '—' }}</span>
          </td>
          <td class="row-actions">
            <button class="link" type="button" @click="openDetail(row)">核对来源</button>
            <button
              v-if="row['就绪状态'] === '等待超时'"
              class="link"
              type="button"
              @click="recoverBySnapshot(row)"
            >
              恢复等待
            </button>
          </td>
        </tr>
        <tr v-if="!ledgerRows.length">
          <td :colspan="ledgerColumns.length + 1" class="empty-state">暂无依赖就绪快照，可先登记等待事件或重新解析依赖</td>
        </tr>
      </tbody>
    </table>

    <table v-else-if="activeTab === 'watermark'" class="data-table">
      <thead>
        <tr><th v-for="column in watermarkColumns" :key="column">{{ column }}</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in watermarkRows" :key="String(row.id)">
          <td v-for="column in watermarkColumns" :key="column">{{ row[column] ?? '—' }}</td>
        </tr>
        <tr v-if="!watermarkRows.length">
          <td :colspan="watermarkColumns.length" class="empty-state">暂无上游运行水位</td>
        </tr>
      </tbody>
    </table>

    <table v-else-if="activeTab === 'waiting'" class="data-table">
      <thead>
        <tr><th v-for="column in waitingColumns" :key="column">{{ column }}</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in waitingRows" :key="String(row.id)">
          <td v-for="column in waitingColumns" :key="column">
            <span v-if="column === '队列状态'" class="state-tag" :class="stateClass(row[column])">{{ row[column] }}</span>
            <span v-else>{{ row[column] ?? '—' }}</span>
          </td>
        </tr>
        <tr v-if="!waitingRows.length">
          <td :colspan="waitingColumns.length" class="empty-state">等待队列为空</td>
        </tr>
      </tbody>
    </table>

    <table v-else class="data-table">
      <thead>
        <tr><th v-for="column in recoveryColumns" :key="column">{{ column }}</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in recoveryRows" :key="String(row.id)">
          <td v-for="column in recoveryColumns" :key="column">
            <span v-if="Array.isArray(row[column])">{{ row[column].join('、') || '—' }}</span>
            <span v-else>{{ row[column] ?? '—' }}</span>
          </td>
        </tr>
        <tr v-if="!recoveryRows.length">
          <td :colspan="recoveryColumns.length" class="empty-state">暂无重跑恢复留痕</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条就绪快照记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <div v-if="detail" class="modal-mask" @click.self="detail = null">
      <div class="modal-card">
        <header class="modal-head">
          <h3>就绪核对：{{ detail['快照编号'] }}</h3>
          <button class="link" type="button" @click="detail = null">关闭</button>
        </header>
        <dl class="detail-grid">
          <template v-for="item in detailItems" :key="item.label">
            <dt>{{ item.label }}</dt>
            <dd>{{ item.value ?? '—' }}</dd>
          </template>
        </dl>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/depledger'
const ledgerStates = ['等待中', '已就绪', '等待超时', '重跑恢复']
const tabs = [
  { key: 'ledger', label: '就绪快照' },
  { key: 'watermark', label: '上游水位' },
  { key: 'waiting', label: '等待队列' },
  { key: 'recovery', label: '恢复留痕' },
] as const

type TabKey = (typeof tabs)[number]['key']

const ledgerColumns = [
  '快照编号', '就绪状态', '版本号', '上游作业', '下游作业', '业务日期', '上游批次日',
  '偏移量分钟', '等待超时分钟', '采用水位编号', '采用运行编号', '来源时区', '超时原因',
]
const watermarkColumns = ['事件编号', '上游作业', '业务日期', '运行编号', '事件时间', '事件序号', '时区', '触发条件']
const waitingColumns = ['等待编号', '关系编号', '上游作业', '下游作业', '业务日期', '队列状态', '断点序号', '断点说明']
const recoveryColumns = ['恢复编号', '重跑任务编号', '来源快照编号', '来源等待编号', '恢复等待事件', '恢复运行记录', '保留断点序号', '恢复时间']

const activeTab = ref<TabKey>('ledger')
const ledgerRows = ref<Row[]>([])
const watermarkRows = ref<Row[]>([])
const waitingRows = ref<Row[]>([])
const recoveryRows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const keyword = ref('')
const stateFilter = ref('')
const detail = ref<Row | null>(null)

const statCards = ref([
  { label: '就绪快照总数', value: 0 },
  { label: '等待中', value: 0 },
  { label: '已就绪可执行', value: 0 },
  { label: '等待超时', value: 0 },
  { label: '重跑恢复', value: 0 },
])

const detailItems = computed(() => {
  const d = detail.value
  if (!d) return []
  return [
    { label: '快照编号 / 版本', value: `${d['快照编号']}（v${d['版本号']}，上一状态：${d['上一状态'] || '无'}）` },
    { label: '就绪状态', value: d['就绪状态'] },
    { label: '依赖关系', value: `${d['关系编号']}：${d['上游作业']} → ${d['下游作业']}` },
    { label: '业务批次日 / 上游批次日', value: `${d['业务日期']} / ${d['上游批次日']}` },
    { label: '来源偏移量（分钟）', value: d['偏移量分钟'] },
    { label: '等待超时（分钟）与截止', value: `${d['等待超时分钟']}（截止 ${d['等待截止']}）` },
    { label: '来源水位编号', value: d['采用水位编号'] },
    { label: '来源运行编号', value: d['采用运行编号'] },
    { label: '来源时区 / 事件时间', value: `${d['来源时区']} / ${d['来源事件时间']}` },
    { label: '水位裁决版本数', value: d['裁决版本数'] },
    { label: '等待超时原因', value: d['超时原因'] || '未超时' },
    { label: '保留断点', value: `#${d['断点序号']} ${d['断点说明']}` },
  ]
})

function stateClass(state: unknown): string {
  const map: Record<string, string> = {
    已就绪: 'state-ready',
    等待中: 'state-waiting',
    等待超时: 'state-timeout',
    重跑恢复: 'state-recovered',
    已触发: 'state-ready',
    已超时: 'state-timeout',
  }
  return map[String(state)] ?? 'state-waiting'
}

function resetFilters() {
  keyword.value = ''
  stateFilter.value = ''
  void reload()
}

function switchTab(key: TabKey) {
  activeTab.value = key
  void reload()
}

function openDetail(row: Row) {
  detail.value = row
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

async function runRecompute() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/recompute`, { method: 'POST' })
    if (!response.ok) throw new Error('依赖重算未生效，请稍后重试')
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '依赖重算失败'
  }
}

async function recoverBySnapshot(row: Row) {
  errorMessage.value = ''
  try {
    const rerunResp = await request(`${ENDPOINT}/reruns`)
    const rerunPayload = await rerunResp.json()
    const target = (rerunPayload.items ?? []).find((item: Row) => item['来源快照编号'] === row['快照编号'])
    if (!target) {
      errorMessage.value = '该快照未生成异常重跑单，无法恢复'
      return
    }
    const response = await request(`${ENDPOINT}/reruns/${target.id}/recover`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '恢复等待失败')
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复等待失败'
  }
}

async function loadStats() {
  try {
    const response = await request(`${ENDPOINT}/ledger/stats`)
    if (!response.ok) return
    const payload = await response.json()
    statCards.value = (payload.items ?? []).map((item: { label: string; value: number }) => ({ ...item }))
  } catch {
    // 统计失败不阻断主表
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    if (activeTab.value === 'ledger') {
      const query = new URLSearchParams()
      if (keyword.value) query.set('keyword', keyword.value)
      if (stateFilter.value) query.set('state', stateFilter.value)
      const response = await request(`${ENDPOINT}/ledger?${query.toString()}`)
      if (!response.ok) throw new Error('就绪快照读取失败')
      const payload = await response.json()
      ledgerRows.value = payload.items ?? []
      total.value = payload.total ?? ledgerRows.value.length
      await loadStats()
      return
    }
    const path = {
      watermark: '/watermarks',
      waiting: '/waiting',
      recovery: '/recovery',
    }[activeTab.value]
    const response = await request(`${ENDPOINT}${path}`)
    if (!response.ok) throw new Error('账本数据读取失败')
    const payload = await response.json()
    const items: Row[] = payload.items ?? []
    if (activeTab.value === 'watermark') watermarkRows.value = items
    if (activeTab.value === 'waiting') waitingRows.value = items
    if (activeTab.value === 'recovery') recoveryRows.value = items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '依赖就绪账本读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.tab-row { display: flex; gap: 8px; margin-bottom: 12px; }
.tab-btn { border: 1px solid var(--border); background: #fff; border-radius: 6px; padding: 6px 14px; cursor: pointer; font-size: 13px; }
.tab-btn.active { background: var(--brand); border-color: var(--brand); color: #fff; }
.state-tag { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; }
.state-ready { background: #e7f6ec; color: #1a7f37; }
.state-waiting { background: #eef2ff; color: #3538cd; }
.state-timeout { background: #fdecec; color: #b42318; }
.state-recovered { background: #fff4e5; color: #b54708; }
.modal-mask { position: fixed; inset: 0; background: rgba(15, 23, 42, 0.45); display: flex; align-items: center; justify-content: center; z-index: 20; }
.modal-card { background: #fff; border-radius: 10px; width: 640px; max-width: 92vw; max-height: 86vh; overflow: auto; padding: 18px 20px; }
.modal-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
.modal-head h3 { margin: 0; font-size: 16px; }
.detail-grid { display: grid; grid-template-columns: 160px 1fr; gap: 6px 12px; margin: 0; }
.detail-grid dt { color: var(--muted); font-size: 12px; }
.detail-grid dd { margin: 0; font-size: 13px; word-break: break-all; }
</style>
