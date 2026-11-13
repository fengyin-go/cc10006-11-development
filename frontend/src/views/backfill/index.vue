<template>
  <section class="page" data-module="backfill">
    <header class="page-head">
      <div>
        <h2>历史回填管理</h2>
        <p class="page-desc">维护历史回填作业的输入、执行约束与结果记录；关联流作业的回填水位随检查点一起推进。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记回填作业</button>
        <button class="btn" type="button" @click="exportRows">导出历史回填清单</button>
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
          <td v-for="column in tableColumns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="tableColumns.length + 1" class="empty-state">暂无历史回填数据，可先登记回填作业</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条历史回填记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <section v-if="relatedBackfills.length" class="panel" data-panel="backfill-sync">
      <header class="panel-head">
        <div>
          <h3>流作业检查点同步台账</h3>
          <p class="panel-desc">回填水位与流作业最后成功检查点一起推进；未完成检查点不会出现在这里。</p>
        </div>
      </header>
      <table class="data-table compact">
        <thead>
          <tr><th>回填作业</th><th>关联流作业</th><th>回填水位</th><th>同步检查点</th><th>待回填事件</th><th>同步状态</th><th>流作业恢复状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="row in relatedBackfills" :key="String(row.id)">
            <td>{{ row['作业编号'] }}</td>
            <td>{{ row['关联流作业'] }}</td>
            <td>{{ row['回填水位'] }}</td>
            <td>{{ row['同步检查点'] }}</td>
            <td>{{ row['待回填事件'] }}</td>
            <td>
              <span class="tag" :class="row['同步状态'] === '已同步' ? 'tag-ok' : 'tag-warn'">{{ row['同步状态'] }}</span>
            </td>
            <td>{{ row['流作业恢复状态'] }}</td>
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

const ENDPOINT = '/api/backfill'
const columns = ["作业编号", "目标表名", "回填起始", "回填截止", "分区范围", "并行度", "失败续跑", "完成校验"]
const tableColumns = [...columns, "关联流作业", "回填水位", "同步检查点", "待回填事件", "同步状态"]
const actions = ["提交执行", "启动回填", "终止回填", "归档结果"]
const statuses = ["草稿", "待执行", "运行中", "已完成", "已失败"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

const relatedBackfills = computed(() => rows.value.filter((row) => row['关联流作业']))

const stats = computed(() => {
  const running = rows.value.filter((row) => row.status === '运行中').length
  const done = rows.value.filter((row) => row.status === '已完成').length
  return [
    { label: '回填作业总数', value: total.value },
    { label: '运行中作业', value: running },
    { label: '已完成作业', value: done },
  ]
})

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '回填作业登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('历史回填动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '历史回填操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('回填作业列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '历史回填列表读取失败'
  }
}

onMounted(reload)
</script>
