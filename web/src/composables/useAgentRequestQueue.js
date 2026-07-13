import { agentApi } from '@/apis'
import { processRunSseResponse } from '@/composables/useAgentRunStream'
import { handleChatError } from '@/utils/errorHandler'

export function useAgentRequestQueue({ getThreadState, startRunStream, onStreamError }) {
  const removeRequestFromQueue = (ts, requestId) => {
    if (!ts || !ts.queuedRequests) return
    ts.queuedRequests = ts.queuedRequests.filter((r) => r.request_id !== requestId)
  }

  const stopRequestStream = (threadId, requestId) => {
    const ts = getThreadState(threadId)
    const entry = ts?.requestStreams?.[requestId]
    if (!entry) return
    entry.controller?.abort()
    delete ts.requestStreams[requestId]
  }

  const stopAllRequestStreams = (threadId) => {
    const ts = getThreadState(threadId)
    if (!ts?.requestStreams) return
    for (const rid of Object.keys(ts.requestStreams)) {
      stopRequestStream(threadId, rid)
    }
  }

  const cancelRequest = async (threadId, requestId) => {
    const ts = getThreadState(threadId)
    if (!ts || !requestId) return false
    try {
      await agentApi.cancelRequest(requestId)
      stopRequestStream(threadId, requestId)
      removeRequestFromQueue(ts, requestId)
      if (ts.onGoingConv?.msgChunks) {
        delete ts.onGoingConv.msgChunks[requestId]
      }
      return true
    } catch (error) {
      if (error?.name !== 'AbortError') {
        handleChatError(error, 'cancel')
      }
      return false
    }
  }

  const syncQueuedRequests = async (threadId, agentSlug) => {
    const ts = getThreadState(threadId)
    if (!ts) return
    try {
      const resp = await agentApi.listThreadQueuedRequests(threadId, agentSlug)
      ts.queuedRequests = resp?.requests || []
    } catch (e) {
      console.warn('Failed to sync queued requests:', e)
    }
  }

  const startRequestStream = async (threadId, requestId) => {
    if (!threadId || !requestId) return
    const ts = getThreadState(threadId)
    if (!ts) return

    ts.requestStreams = ts.requestStreams || {}
    if (ts.requestStreams[requestId]) return

    const controller = new AbortController()
    const entry = { controller, position: 0, status: 'queued' }
    ts.requestStreams[requestId] = entry

    try {
      const response = await agentApi.streamRequestEvents(requestId, {
        signal: controller.signal
      })
      if (!response.ok) {
        throw new Error(`Request SSE response not ok: ${response.status}`)
      }

      const handleEvent = (event, data) => {
        // 一次性取 ts/entry，避免每个分支重复 getThreadState 触发响应式追踪。
        const tsInner = getThreadState(threadId)
        const innerEntry = tsInner?.requestStreams?.[requestId]
        if (!tsInner || innerEntry?.controller !== controller) return

        if (event === 'queued' && data) {
          entry.position = data.position || entry.position
          const queuedRequest = tsInner.queuedRequests?.find((r) => r.request_id === requestId)
          if (queuedRequest) queuedRequest.queue_position = entry.position
        } else if (event === 'run_created' && data) {
          entry.status = 'dispatched'
          if (data.run_id) {
            removeRequestFromQueue(tsInner, requestId)
            stopRequestStream(threadId, requestId)
            void startRunStream(threadId, data.run_id, requestId)
          }
        } else if (event === 'cancelled' || event === 'rejected' || event === 'failed') {
          entry.status = event
          tsInner.isStreaming = false
          tsInner.replyLoadingVisible = false
          tsInner.pendingRequestId = null
          delete tsInner.onGoingConv.msgChunks[requestId]
          removeRequestFromQueue(tsInner, requestId)
          stopRequestStream(threadId, requestId)
          if (typeof onStreamError === 'function') {
            onStreamError(threadId, requestId, event)
          }
        }
      }

      await processRunSseResponse(response, handleEvent)
    } catch (error) {
      if (error?.name !== 'AbortError') {
        console.error('Request SSE stream error:', error)
        handleChatError(error, 'stream')
      }
    } finally {
      const tsFinal = getThreadState(threadId)
      if (tsFinal?.requestStreams?.[requestId]?.controller === controller) {
        delete tsFinal.requestStreams[requestId]
      }
    }
  }

  return {
    startRequestStream,
    stopAllRequestStreams,
    cancelRequest,
    syncQueuedRequests
  }
}
