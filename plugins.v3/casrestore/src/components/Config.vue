<script setup>
import { ref, onMounted, watch } from 'vue'

const props = defineProps({
  initialConfig: { type: Object, default: () => ({}) },
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function, default: null },
})

const emit = defineEmits(['save', 'close', 'switch'])

const cfg = ref({
  enabled: false,
  watch_dir: '',
  account_id: '',
  scope: 'personal',
  family_id: '',
  target_folder_path: '',
  notify: true,
  onlyonce: false,
  overwrite: '0',
  ...(props.initialConfig || {}),
})

const accounts = ref([])
const families = ref([])
const accountHint = ref('')
const familyHint = ref('')
const loading = ref(false)
const saving = ref(false)
const clearing = ref(false)

async function clearRun() {
  clearing.value = true
  try {
    await props.api.post(`plugin/${props.pluginId}/clear_run`)
  } catch (e) {
    console.error('CasRestore clear_run failed', e)
  } finally {
    clearing.value = false
  }
}

async function loadAccounts() {
  try {
    const r = await props.api.get(`plugin/${props.pluginId}/accounts`)
    if (r && r.success) {
      accounts.value = r.items || []
      accountHint.value = `共 ${accounts.value.length} 个账号`
      if (!cfg.value.account_id && accounts.value.length) {
        cfg.value.account_id = accounts.value[0].value
      }
    } else {
      accounts.value = []
      accountHint.value = (r && r.message) || '拉取失败'
    }
  } catch (e) {
    accountHint.value = '拉取失败：' + (e && e.message ? e.message : e)
  }
}

async function loadFamilies() {
  if (!cfg.value.account_id || cfg.value.scope !== 'family') {
    families.value = []
    familyHint.value = cfg.value.scope !== 'family' ? '个人空间无需选择家庭' : '先选择账号'
    return
  }
  try {
    const r = await props.api.get(`plugin/${props.pluginId}/families`, {
      params: { account_id: cfg.value.account_id },
    })
    if (r && r.success) {
      families.value = r.items || []
      familyHint.value = `共 ${families.value.length} 个家庭`
    } else {
      families.value = []
      familyHint.value = (r && r.message) || '拉取失败'
    }
  } catch (e) {
    families.value = []
    familyHint.value = '拉取失败：' + (e && e.message ? e.message : e)
  }
}

function save() {
  saving.value = true
  try {
    // 主应用监听 save 事件接管配置保存，无需插件自己调用后端保存接口
    emit('save', cfg.value)
  } finally {
    setTimeout(() => {
      saving.value = false
    }, 500)
  }
}

watch(() => cfg.value.account_id, loadFamilies)
watch(() => cfg.value.scope, loadFamilies)

onMounted(async () => {
  loading.value = true
  try {
    await loadAccounts()
    await loadFamilies()
  } finally {
    loading.value = false
  }
})
</script>

<template>
  <div class="pa-4">
    <v-card>
      <v-card-title>CAS 恢复源文件 · 设置</v-card-title>
      <v-card-text>
        <v-alert type="info" variant="tonal" density="compact" class="mb-4">
          监控目录有新 .cas 写入时自动恢复；CAS 服务地址与 API Key 读取「CAS助手」插件，账号 / 家庭 / 目标路径在此选择。
        </v-alert>
        <v-row>
          <v-col cols="12" md="3">
            <v-switch v-model="cfg.enabled" label="启用插件" density="compact" hide-details />
          </v-col>
          <v-col cols="12" md="3">
            <v-switch v-model="cfg.notify" label="发送通知" density="compact" hide-details />
          </v-col>
          <v-col cols="12" md="3">
            <v-switch v-model="cfg.onlyonce" label="立即运行一次" density="compact" hide-details />
          </v-col>
          <v-col cols="12" md="3" class="d-flex align-center">
            <v-btn color="warning" variant="outlined" block :loading="clearing" @click="clearRun">
              清除运行记录
            </v-btn>
          </v-col>
          <v-col cols="12">
            <v-text-field
              v-model="cfg.watch_dir"
              label="监控目录"
              hint="容器内绝对路径（含子目录）"
              persistent-hint
              density="compact"
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-select
              v-model="cfg.account_id"
              :items="accounts"
              label="恢复账号"
              :hint="accountHint"
              persistent-hint
              density="compact"
              item-title="label"
              item-value="value"
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-select
              v-model="cfg.scope"
              :items="[
                { title: '个人空间', value: 'personal' },
                { title: '家庭空间', value: 'family' },
              ]"
              label="恢复位置"
              item-title="title"
              item-value="value"
              density="compact"
              hide-details
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-select
              v-model="cfg.family_id"
              :items="families"
              label="家庭列表"
              :hint="familyHint"
              persistent-hint
              density="compact"
              item-title="label"
              item-value="value"
            />
          </v-col>
          <v-col cols="12">
            <v-text-field
              v-model="cfg.target_folder_path"
              label="目标文件夹路径"
              hint="网盘基路径（如 家庭1/测试），本地子目录自动拼接到其后"
              persistent-hint
              density="compact"
            />
          </v-col>
          <v-col cols="12">
            <v-select
              v-model="cfg.overwrite"
              :items="[
                { title: '跳过（网盘已有同名文件时不恢复）', value: '0' },
                { title: '覆盖（网盘已有同名文件时覆盖）', value: '1' },
              ]"
              label="同名文件"
              hint="恢复时若网盘已存在同名文件，选择跳过还是覆盖"
              persistent-hint
              density="compact"
              item-title="title"
              item-value="value"
            />
          </v-col>
        </v-row>
      </v-card-text>
      <v-card-actions>
        <v-spacer />
        <v-btn color="primary" :loading="saving" @click="save">保存配置</v-btn>
      </v-card-actions>
    </v-card>
  </div>
</template>
