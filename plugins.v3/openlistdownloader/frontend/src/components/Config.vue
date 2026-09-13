<script setup lang="ts">
import { ref, onMounted } from 'vue'

/**
 * 插件设置远程组件（./Config）。
 * 契约（来自 MoviePilot-Frontend 的 PluginConfigDialog.vue）：
 *  - 主前端调 GET /api/v1/plugin/form/{id} 得到 { model }，通过 :initial-config 注入
 *  - 本组件渲染表单，保存时 emit('save', newConfig)，由主前端统一 PUT /api/v1/plugin/{id} 保存
 */
const props = defineProps({
  // kebab-case "initial-config" 会被 Vue 映射为 initialConfig
  initialConfig: { type: Object, default: () => ({}) },
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
})

const emit = defineEmits(['save', 'layout', 'switch', 'close'])

const form = ref<Record<string, any>>({ ...(props.initialConfig || {}) })

// 系统已配置的通知渠道（供「通知渠道」下拉框选择）
const wxChannels = ref<Array<{ name: string; type: string }>>([])
const wxLoading = ref(false)

const PLUGIN_ID = 'OpenListDownloader'

const CHANNEL_TYPE_LABEL: Record<string, string> = {
  wechat: '企业微信',
  wechatclawbot: '微信机器人',
  telegram: 'Telegram',
  feishu: '飞书',
  slack: 'Slack',
  discord: 'Discord',
  vocechat: 'VoceChat',
  synologychat: 'SynologyChat',
  qqbot: 'QQ',
}

function channelLabel(c: { name: string; type: string }) {
  const t = CHANNEL_TYPE_LABEL[c.type] || c.type || '未知'
  return `${c.name}（${t}）`
}

async function loadChannels() {
  wxLoading.value = true
  try {
    let data: any = null
    if (props.api && typeof props.api.get === 'function') {
      data = await props.api.get(`plugin/${PLUGIN_ID}/channels`)
    } else {
      const res = await fetch(`/api/v1/plugin/${PLUGIN_ID}/channels`)
      data = await res.json()
    }
    const list = data?.channels || (Array.isArray(data) ? data : [])
    wxChannels.value = (list || []).filter((c: any) => c && c.name).map((c: any) => ({
      name: c.name,
      type: c.type || '',
    }))
  } catch (e) {
    wxChannels.value = []
  } finally {
    wxLoading.value = false
  }
}

// 开关项：显示在顶部，一行 4 个
const switches = [
  { key: 'enabled', label: '启用插件' },
  { key: 'onlyonce', label: '立即运行一次' },
  { key: 'clear_records', label: '清空记录' },
  { key: 'notify', label: '发送通知' },
]

// 输入项：显示在开关下方，一行 2 个（monitor_paths 放最下面）
const inputs = [
  { key: 'openlist_url', label: 'OpenList 地址', type: 'text', placeholder: 'http://192.168.2.6:5244' },
  { key: 'openlist_token', label: 'OpenList Token', type: 'text', placeholder: 'API 令牌 / JWT' },
  { key: 'downloader', label: '下载器名称', type: 'text', placeholder: 'Aria2' },
  { key: 'wx_channel', label: '通知渠道（留空=MP默认通知）', type: 'channel', placeholder: '选择系统已配置的通知渠道' },
  { key: 'download_dir', label: 'Aria2 下载保存目录', type: 'text', placeholder: '/downloads' },
  { key: 'file_ext', label: '仅下载后缀（逗号分隔，空=全部）', type: 'text', placeholder: 'mkv,mp4' },
  { key: 'min_size', label: '最小文件大小（MB，0=不限）', type: 'number', placeholder: '0' },
  { key: 'max_depth', label: '递归深度（0=仅当前层）', type: 'number', placeholder: '0' },
  { key: 'max_files_per_round', label: '单轮最多下载数', type: 'number', placeholder: '50' },
  {
    key: 'cron_expression',
    label: '定时表达式',
    type: 'select',
    options: [
      { value: '*/10 * * * *', text: '每 10 分钟' },
      { value: '*/30 * * * *', text: '每 30 分钟' },
      { value: '0 * * * *', text: '每小时' },
      { value: '0 */2 * * *', text: '每 2 小时' },
      { value: '0 */4 * * *', text: '每 4 小时' },
      { value: '0 0 * * *', text: '每天' },
      { value: '0 2 * * *', text: '每天凌晨 2 点' },
      { value: '0 0 * * 0', text: '每周日' },
      { value: '20 7-11 * * *', text: '每天下午7:20-11:20' },
    ],
  },
  { key: 'monitor_paths', label: '监控路径（每行一个）', type: 'textarea', placeholder: '/电影\n/剧集/更新', rows: 4 },
]

// 下拉框（combobox）值变化处理：
//  - 选中预设项（对象）→ 取其 value（cron 表达式）
//  - 手动输入自定义 cron（字符串）→ 原样保存
function onCronChange(f: any, v: any) {
  if (v && typeof v === 'object' && v.value !== undefined) {
    form.value[f.key] = v.value
  } else {
    form.value[f.key] = v ?? ''
  }
}

onMounted(() => {
  // 主前端已通过 initial-config 注入配置；若为空则兜底空对象
  form.value = { ...(props.initialConfig || {}) }
  // 拉取系统已配置的企业微信通知渠道
  loadChannels()
})

function save() {
  emit('save', { ...form.value })
}
</script>

<template>
  <div class="pa-4 config-page">
    <!-- 右上角关闭按钮 -->
    <v-btn
      icon="mdi-close"
      variant="text"
      size="small"
      class="config-close"
      @click="emit('close')"
    />
    <div class="text-h6 font-weight-bold mb-4">
      <v-icon icon="mdi-cog-outline" class="mr-2" />
      OpenList 自动下载 · 设置
    </div>

    <!-- 开关区：一行 4 个 -->
    <v-row>
      <v-col v-for="f in switches" :key="f.key" cols="12" sm="6" md="3">
        <v-card variant="tonal" flat class="px-2 py-1 mb-2">
          <v-switch
            :model-value="!!form[f.key]"
            :label="f.label"
            color="primary"
            density="comfortable"
            hide-details
            @update:model-value="v => (form[f.key] = !!v)"
          />
        </v-card>
      </v-col>
    </v-row>

    <v-divider class="my-4" />

    <!-- 输入区：一行 2 个 -->
    <v-row>
      <v-col v-for="f in inputs" :key="f.key" cols="12" md="6">
        <!-- 多行文本 -->
        <v-textarea
          v-if="f.type === 'textarea'"
          :model-value="form[f.key] ?? ''"
          :label="f.label"
          :placeholder="f.placeholder"
          :rows="f.rows || 3"
          auto-grow
          :hide-details="false"
          variant="outlined"
          density="comfortable"
          @update:model-value="v => (form[f.key] = v)"
        />
        <!-- 数字 -->
        <v-text-field
          v-else-if="f.type === 'number'"
          :model-value="form[f.key] ?? 0"
          :label="f.label"
          :placeholder="f.placeholder"
          type="number"
          variant="outlined"
          density="comfortable"
          @update:model-value="v => (form[f.key] = Number(v) || 0)"
        />
        <!-- 下拉框（可输入自定义 cron） -->
        <v-combobox
          v-else-if="f.type === 'select'"
          :model-value="form[f.key] ?? ''"
          :label="f.label"
          :items="f.options"
          item-title="text"
          item-value="value"
          variant="outlined"
          density="comfortable"
          :hide-details="false"
          @update:model-value="v => onCronChange(f, v)"
        />
        <!-- 微信通知渠道下拉框（选项来自系统配置） -->
        <v-select
          v-else-if="f.type === 'channel'"
          :model-value="form[f.key] ?? ''"
          :label="f.label"
          :items="wxChannels"
          :item-title="(c: any) => channelLabel(c)"
          item-value="name"
          :loading="wxLoading"
          :placeholder="f.placeholder"
          clearable
          variant="outlined"
          density="comfortable"
          :hide-details="false"
          @update:model-value="v => (form[f.key] = v || '')"
        />
        <!-- 文本 -->
        <v-text-field
          v-else
          :model-value="form[f.key] ?? ''"
          :label="f.label"
          :placeholder="f.placeholder"
          variant="outlined"
          density="comfortable"
          @update:model-value="v => (form[f.key] = v)"
        />
      </v-col>
    </v-row>

    <!-- 保存按钮：右下角 -->
    <div class="d-flex justify-end mt-4">
      <v-btn color="primary" prepend-icon="mdi-content-save" @click="save">
        保存
      </v-btn>
    </div>
  </div>
</template>

<style scoped>
.config-page {
  position: relative;
}
.config-close {
  position: absolute;
  top: 4px;
  right: 4px;
  z-index: 2;
}
.cron-row {
  margin-top: 4px;
}
.cron-presets {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 8px;
  padding: 6px 4px;
  font-size: 13px;
  color: rgba(var(--v-theme-on-surface), 0.6);
}
.cron-presets__prefix {
  font-size: 13px;
  margin-right: 2px;
}
.cron-presets__btn {
  text-transform: none;
}
</style>
