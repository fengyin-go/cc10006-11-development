<template>
  <section class="page" data-module="stream">
    <header class="page-head">
      <div>
        <h2>流式处理管理</h2>
        <p class="page-desc">维护流处理作业的输入、执行约束与结果记录，按输入 Topic 持久化检查点并从最后成功水位恢复。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记流处理作业</button>
        <button class="btn" type="button" @click="queryRecords">查询恢复记录</button>
        <button class="btn" type="button" @click="exportRows">导出流式处理清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in tableColumns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in tableColumns" :key="column">{{ formatCell(row[column]) }}</td>
          <td class="row-actions">
            <button
              v-for="action in rowActions(row)"
              :key="action"
              class="link"
              type="button"
              @click="openAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="tableColumns.length + 1" class="empty-state">暂无流式处理数据，可先登记流处理作业</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条流式处理记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <section v-if="panel" class="panel" data-panel="recovery">
      <header class="panel-head">
        <div>
          <h3>检查点恢复面板 · {{ panel['作业编号'] }}</h3>
          <p class="panel-desc">
            输入 Topic {{ panel['输入Topic'] }} → 输出 Topic {{ panel['输出Topic'] }}
            <span class="tag" :class="panel['已恢复'] ? 'tag-ok' : 'tag-warn'">{{ panel['恢复状态'] }}</span>
          </p>
        </div>
        <div class="page-actions">
          <button class="btn" type="button" @click="reloadPanel">刷新进度</button>
          <button class="btn ghost" type="button" @click="panel = null">收起</button>
        </div>
      </header>

      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">最后成功水位</span>
          <strong class="stat-value">{{ panel['最后成功水位'] ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">检查点间隔（秒）</span>
          <strong class="stat-value">{{ panel['检查点间隔'] ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">剩余待处理事件</span>
          <strong class="stat-value">{{ panel['剩余待处理事件'] ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">输出回执水位</span>
          <strong class="stat-value">{{ panel['输出回执']?.['回执水位'] ?? 0 }}</strong>
        </article>
      </div>

      <div v-if="panel['未完成检查点']?.length" class="notice" data-note="incomplete">
        存在未完成检查点，恢复按钮已按最后成功检查点回退：{{ panel['未完成检查点'].map((item: any) => item['检查点编号']).join('、') }}
      </div>

      <div class="panel-grid">
        <article>
          <h4>本次检查点</h4>
          <dl v-if="panel['本次检查点']">
            <dt>检查点编号</dt><dd>{{ panel['本次检查点']['检查点编号'] }}</dd>
            <dt>水位</dt><dd>{{ panel['本次检查点']['水位'] }}</dd>
            <dt>输出编号区间</dt><dd>{{ rangeText(panel['本次检查点']['输出编号区间']) }}</dd>
            <dt>状态</dt><dd>{{ panel['本次检查点']['状态'] }}</dd>
          </dl>
          <p v-else class="muted">暂无检查点</p>
        </article>
        <article>
          <h4>上一次成功检查点</h4>
          <dl v-if="panel['上一次成功检查点']">
            <dt>检查点编号</dt><dd>{{ panel['上一次成功检查点']['检查点编号'] }}</dd>
            <dt>水位</dt><dd>{{ panel['上一次成功检查点']['水位'] }}</dd>
            <dt>完成时间</dt><dd>{{ panel['上一次成功检查点']['完成时间'] }}</dd>
          </dl>
          <p v-else class="muted">暂无更早的成功检查点</p>
        </article>
        <article>
          <h4>输出回执</h4>
          <dl v-if="panel['输出回执']">
            <dt>已写区间</dt><dd>{{ panel['输出回执']['写入区间数'] }}</dd>
            <dt>重复写入</dt><dd>{{ panel['输出回执']['重复写入'] }}</dd>
            <dt>去重事件</dt><dd>{{ panel['输出回执']['已去重事件'] }}</dd>
            <dt>与检查点一致</dt><dd>{{ panel['输出回执']['与检查点一致'] ? '是' : '否' }}</dd>
          </dl>
        </article>
        <article>
          <h4>下游回填台账</h4>
          <dl v-for="item in panel['回填台账'] ?? []" :key="String(item['作业编号'])">
            <dt>{{ item['作业编号'] }}</dt>
            <dd>回填水位 {{ item['回填水位'] }} · 同步检查点 {{ item['同步检查点'] }} · 待回填 {{ item['待回填事件'] }}</dd>
          </dl>
          <p v-if="!panel['回填台账']?.length" class="muted">没有关联的回填作业</p>
        </article>
      </div>

      <h4>检查点记录</h4>
      <table class="data-table compact">
        <thead>
          <tr><th>检查点编号</th><th>水位</th><th>起止水位</th><th>事件数</th><th>输出编号区间</th><th>状态</th><th>完成时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in panel['检查点记录'] ?? []" :key="item['检查点编号']">
            <td>{{ item['检查点编号'] }}</td>
            <td>{{ item['水位'] }}</td>
            <td>{{ item['起始水位'] }} → {{ item['水位'] }}</td>
            <td>{{ item['事件数'] }}</td>
            <td>{{ rangeText(item['输出编号区间']) }}</td>
            <td>
              <span class="tag" :class="item['已完成'] ? 'tag-ok' : 'tag-bad'">{{ item['状态'] }}</span>
            </td>
            <td>{{ item['完成时间'] ?? '—' }}</td>
          </tr>
          <tr v-if="!panel['检查点记录']?.length">
            <td colspan="7" class="empty-state">暂无检查点记录</td>
          </tr>
        </tbody>
      </table>

      <h4>恢复实例</h4>
      <table class="data-table compact">
        <thead>
          <tr><th>实例编号</th><th>状态</th><th>起点检查点</th><th>起点水位</th><th>剩余事件</th><th>发起时间</th><th>完成时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in panel['恢复实例记录'] ?? []" :key="String(item['instance_id'])">
            <td>{{ item['instance_id'] }}</td>
            <td>{{ item['status'] }}</td>
            <td>{{ item['from_checkpoint'] }}</td>
            <td>{{ item['from_watermark'] }}</td>
            <td>{{ item['remaining_events'] }}</td>
            <td>{{ item['requested_at'] }}</td>
            <td>{{ item['completed_at'] ?? '—' }}</td>
          </tr>
          <tr v-if="!panel['恢复实例记录']?.length">
            <td colspan="7" class="empty-state">尚未发起恢复</td>
          </tr>
        </tbody>
      </table>
    </section>

    <dialog v-if="dialog" class="action-dialog" open @cancel.prevent="dialog = null">
      <h3>{{ dialog.action }} · {{ dialog.row['作业编号'] }}</h3>
      <p class="panel-desc">提交后检查点、输出回执、作业状态与回填台账一起推进；失败会回到可恢复状态。</p>
      <form @submit.prevent="submitAction">
        <label v-if="dialog.action === '提交检查点'" class="filter-item">
          <span>目标水位（整数）</span>
          <input v-model="dialog.watermark" type="text" placeholder="例如 10000" />
        </label>
        <label class="filter-item">
          <span>检查点间隔（秒）</span>
          <input v-model="dialog.interval" type="text" placeholder="例如 30" />
        </label>
        <label v-if="dialog.action !== '提交检查点'" class="filter-item">
          <span>恢复水位（不超过最后成功水位）</span>
          <input v-model="dialog.watermark" type="text" placeholder="留空则从最后成功检查点继续" />
        </label>
        <label v-if="dialog.action === '恢复作业'" class="check-item">
          <input v-model="dialog.simulateFailure" type="checkbox" />
          <span>模拟恢复中断（验证回到可恢复状态）</span>
        </label>
        <label v-else-if="dialog.action === '提交检查点'" class="check-item">
          <input v-model="dialog.simulateFailure" type="checkbox" />
          <span>模拟输出写入失败（验证回滚）</span>
        </label>
        <p v-if="dialogMessage" class="error-text">{{ dialogMessage }}</p>
        <div class="page-actions">
          <button class="btn primary" type="submit">确认{{ dialog.action }}</button>
          <button class="btn ghost" type="button" @click="dialog = null">取消</button>
        </div>
      </form>
    </dialog>

    <section v-if="records.length" class="panel" data-panel="records">
      <header class="panel-head">
        <h3>恢复记录查询结果</h3>
        <button class="btn ghost" type="button" @click="records = []">收起</button>
      </header>
      <table class="data-table compact">
        <thead>
          <tr><th>作业编号</th><th>恢复状态</th><th>最后成功水位</th><th>检查点间隔</th><th>剩余待处理事件</th><th>本次检查点</th><th>上一次成功检查点</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in records" :key="String(item['作业编号'])">
            <td>{{ item['作业编号'] }}</td>
            <td>{{ item['恢复状态'] }}</td>
            <td>{{ item['最后成功水位'] }}</td>
            <td>{{ item['检查点间隔'] }}</td>
            <td>{{ item['剩余待处理事件'] }}</td>
            <td>{{ checkpointId(item['本次检查点']) }}</td>
            <td>{{ checkpointId(item['上一次成功检查点']) }}</td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type Progress = Record<string, any>

const ENDPOINT = '/api/stream'
const columns = ["作业编号", "输入Topic", "处理逻辑", "窗口类型", "窗口时长秒", "水位延迟", "检查点间隔", "输出Topic"]
const tableColumns = [...columns, "已确认水位", "最后成功检查点", "恢复状态", "剩余待处理事件"]
const baseActions = ["提交校验", "启动作业", "停止作业"]
const checkpointActions = ["提交检查点", "恢复作业"]
// 检查点动作需要弹窗填写水位与间隔；基础动作直接提交
const directActions = new Set(["提交校验", "启动作业", "停止作业"])

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const panel = ref<Progress | null>(null)
const records = ref<Row[]>([])
const dialog = ref<{ action: string; row: Row; watermark: string; interval: string; simulateFailure: boolean } | null>(null)
const dialogMessage = ref('')

const stats = computed(() => {
  const running = rows.value.filter((row) => row.恢复状态 === '已恢复').length
  const pending = rows.value.filter((row) => row['恢复状态'] === '可恢复' || row['恢复状态'] === '恢复失败').length
  return [
    { label: '流处理作业总数', value: total.value },
    { label: '运行中作业', value: running },
    { label: '待恢复作业', value: pending },
  ]
})

function formatCell(value: string | number | null | undefined) {
  return value === undefined || value === null || value === '' ? '—' : value
}

function checkpointId(checkpoint: unknown) {
  if (!checkpoint || typeof checkpoint !== 'object') return '—'
  return (checkpoint as Record<string, any>)['检查点编号'] ?? '—'
}

function rangeText(range: [number, number] | undefined) {
  return range ? `${range[0]} ~ ${range[1]}` : '—'
}

function rowActions(row: Row) {
  return [...baseActions, ...checkpointActions]
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '流处理作业登记入口尚未接入审批流'
}

function openAction(action: string, row: Row) {
  if (directActions.has(action)) {
    void runDirectAction(action, row)
    return
  }
  dialogMessage.value = ''
  dialog.value = {
    action,
    row,
    watermark: '',
    interval: String(row.检查点间隔 ?? 30),
    simulateFailure: false,
  }
  void loadPanel(row)
}

async function runDirectAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('流式处理动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '流式处理操作失败'
  }
}

async function loadPanel(row: Row) {
  try {
    const response = await request(`/api/stream/recovery/${row.id}`)
    if (response.ok) {
      panel.value = await response.json()
    }
  } catch {
    // 面板只做辅助展示，读取失败不打断列表操作
  }
}

async function reloadPanel() {
  if (!panel.value) return
  const match = rows.value.find((row) => row.作业编号 === panel.value?.['作业编号'])
  if (!match) return
  await loadPanel(match)
}

async function submitAction() {
  if (!dialog.value) return
  const current = dialog.value
  const values: Record<string, unknown> = { action: current.action }
  if (current.watermark.trim()) {
    values.watermark = current.watermark.trim()
  }
  if (current.interval.trim()) {
    values.checkpoint_interval = current.interval.trim()
  }
  if (current.simulateFailure) {
    if (current.action === '提交检查点') {
      values.simulate_output_failure = true
    } else {
      values.simulate_failure = true
    }
    values.force = true
  }
  try {
    const response = await request(`${ENDPOINT}/${current.row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) {
      dialogMessage.value = payload?.message ?? '动作未生效，请稍后重试'
      return
    }
    dialogMessage.value = ''
    dialog.value = null
    await reload()
    if (payload.entry?.['恢复进度']) {
      panel.value = payload.entry['恢复进度']
    } else {
      await loadPanel(current.row)
    }
  } catch (error) {
    dialogMessage.value = error instanceof Error ? error.message : '流式处理操作失败'
  }
}

async function queryRecords() {
  const jobNo = String(filters.value['作业编号'] ?? '').trim()
  try {
    const response = await request(`/api/stream/recovery${jobNo ? `?job_no=${encodeURIComponent(jobNo)}` : ''}`)
    if (!response.ok) throw new Error('恢复记录读取失败')
    const payload = await response.json()
    records.value = payload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复记录读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('流处理作业列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '流式处理列表读取失败'
  }
}

onMounted(async () => {
  await reload()
  const running = rows.value.find((row) => row.恢复状态 === '已恢复')
  if (running) await loadPanel(running)
})
</script>
