package reduction

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

type ToolReductionMiddlewareConfig struct {
	ToolTruncation *ToolTruncation

	ToolOffload *ToolOffload
}

type ToolTruncation struct {
	ToolTruncationConfigMapping map[string]ToolTruncationConfig
}

// ToolTruncationConfig configures how a tool's result is truncated
type ToolTruncationConfig struct {
	// provide one of the configs at least
	MaxLength *int

	MaxLineLength *int
}

type ToolOffload struct {
	Tokenizer func(msg adk.Message) (int64, error)

	ToolOffloadThreshold *ToolOffloadThresholdConfig
	// ToolOffloadThreshold func(ctx context.Context, state *adk.ChatModelAgentState, iterCount int) (rangeStart, rangeEnd int, needOffloading bool)

	// ToolOffloadConfigMapping maps tool names to their offloading configurations.
	ToolOffloadConfigMapping map[string]ToolOffloadConfig

	// ToolOffloadPostProcess processes the agent state after offloading, called once per reduction.
	ToolOffloadPostProcess func(ctx context.Context, state *adk.ChatModelAgentState) context.Context
}

type ToolOffloadThresholdConfig struct {
	// optional, default 20k
	MaxTokens int64

	// messages except system
	// optional, default 5
	OffloadBatchSize int

	// optional, default 0
	RetentionSuffixLimit int
}

// ToolOffloadConfig configures how a tool's result is offloaded
type ToolOffloadConfig struct {
	// OffloadBackend is the backend interface used to store offloaded content
	OffloadBackend Backend

	OffloadHandler func(ctx context.Context, detail *ToolDetail) (*OffloadInfo, error)
}

// ToolDetail contains details about a tool's input and output
type ToolDetail struct {
	// Input is the tool's input parameters
	Input *compose.ToolInput
	// Output is the tool's execution result
	Output *compose.ToolOutput
}

type OffloadInfo struct {
	NeedOffload bool

	FilePath string

	OffloadContent string
}

// NewToolReductionMiddleware creates tool reduction middleware from config
func NewToolReductionMiddleware(_ context.Context, config *ToolReductionMiddlewareConfig) (mw adk.ChatModelAgentMiddleware, err error) {
	if config.ToolTruncation == nil && config.ToolOffload == nil {
		return mw, fmt.Errorf("at least provide one of ToolTruncationMapping or ToolOffloadMapping")
	}
	if config.ToolOffload != nil {
		if config.ToolOffload.ToolOffloadThreshold == nil {
			return mw, fmt.Errorf("ToolOffload.toolOffloadThreshold is required")
		}
	}

	return &toolReductionMiddleware{config: config}, nil
}

// NewDefaultToolReductionMiddleware creates default tool reduction middleware
func NewDefaultToolReductionMiddleware(ctx context.Context, tools []tool.BaseTool) (adk.ChatModelAgentMiddleware, error) {
	truncMaxLength := 30000
	backend := filesystem.NewInMemoryBackend()
	offloadRootDir := "/tmp"
	config := &ToolReductionMiddlewareConfig{
		ToolTruncation: &ToolTruncation{
			ToolTruncationConfigMapping: make(map[string]ToolTruncationConfig, len(tools)),
		},
		ToolOffload: &ToolOffload{
			Tokenizer: defaultTokenizer,
			ToolOffloadThreshold: &ToolOffloadThresholdConfig{
				MaxTokens:        300000,
				OffloadBatchSize: 5,
			},
			ToolOffloadConfigMapping: make(map[string]ToolOffloadConfig, len(tools)),
		},
	}

	for _, t := range tools {
		info, err := t.Info(ctx)
		if err != nil {
			return nil, err
		}
		config.ToolTruncation.ToolTruncationConfigMapping[info.Name] = ToolTruncationConfig{MaxLength: &truncMaxLength}
		config.ToolOffload.ToolOffloadConfigMapping[info.Name] = ToolOffloadConfig{
			OffloadBackend: backend,
			OffloadHandler: defaultOffloadHandler(offloadRootDir),
		}
	}

	return &toolReductionMiddleware{
		BaseChatModelAgentMiddleware: adk.BaseChatModelAgentMiddleware{},
		config:                       config,
	}, nil
}

type toolReductionMiddleware struct {
	adk.BaseChatModelAgentMiddleware

	config *ToolReductionMiddlewareConfig
}

func (t *toolReductionMiddleware) WrapInvokableToolCall(endpoint adk.InvokableToolCallEndpoint, tCtx *adk.ToolContext) adk.InvokableToolCallEndpoint {
	toolName := tCtx.Name
	config := t.config.ToolTruncation
	if config == nil || config.ToolTruncationConfigMapping == nil {
		return endpoint
	}
	tc, found := config.ToolTruncationConfigMapping[tCtx.Name]
	if !found {
		return endpoint
	}

	return func(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (string, error) {
		output, err := endpoint(ctx, argumentsInJSON, opts...)
		if err != nil {
			return "", err
		}

		detail := &ToolDetail{
			Input: &compose.ToolInput{
				Name:        toolName,
				Arguments:   argumentsInJSON,
				CallID:      tCtx.CallID,
				CallOptions: opts,
			},
			Output: &compose.ToolOutput{
				Result: output,
			},
		}
		return t.toolTruncationHandler(ctx, tc, detail), nil
	}
}

func (t *toolReductionMiddleware) WrapStreamableToolCall(endpoint adk.StreamableToolCallEndpoint, tCtx *adk.ToolContext) adk.StreamableToolCallEndpoint {
	toolName := tCtx.Name
	config := t.config.ToolTruncation
	if config == nil || config.ToolTruncationConfigMapping == nil {
		return endpoint
	}
	tc, found := config.ToolTruncationConfigMapping[tCtx.Name]
	if !found {
		return endpoint
	}

	return func(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (*schema.StreamReader[string], error) {
		output, err := endpoint(ctx, argumentsInJSON, opts...)
		if err != nil {
			return nil, err
		}

		sr, sw := schema.Pipe[string](10)
		go func() {
			defer sw.Close()

			result := strings.Builder{}
			for {
				chunk, err := output.Recv()
				if err != nil {
					if err != io.EOF {
						sw.Send("", err)
					}
					break
				}
				result.WriteString(chunk)
			}

			detail := &ToolDetail{
				Input: &compose.ToolInput{
					Name:        toolName,
					Arguments:   argumentsInJSON,
					CallID:      tCtx.CallID,
					CallOptions: opts,
				},
				Output: &compose.ToolOutput{
					Result: result.String(),
				},
			}
			truncResult := t.toolTruncationHandler(ctx, tc, detail)
			sw.Send(truncResult, nil)
		}()

		return sr, nil
	}
}

func (t *toolReductionMiddleware) toolTruncationHandler(ctx context.Context, config ToolTruncationConfig, detail *ToolDetail) (truncResult string) {
	if config.MaxLineLength != nil {
		sb := strings.Builder{}
		lines := strings.Split(detail.Output.Result, "\n")
		for i, line := range lines {
			if i > 0 {
				sb.WriteString("\n")
			}

			if config.MaxLength != nil {
				s, truncated := truncateIfTooLong(*config.MaxLength-sb.Len(), contentTruncFmt, line)
				if truncated {
					sb.WriteString(s)
					break
				}
			}

			s, _ := truncateIfTooLong(*config.MaxLineLength, lineTruncFmt, line)
			sb.WriteString(s)
		}
		return sb.String()
	} else if config.MaxLength != nil {
		truncResult, _ = truncateIfTooLong(*config.MaxLength, contentTruncFmt, detail.Output.Result)
		return truncResult
	} else {
		return detail.Output.Result
	}
}

func truncateIfTooLong(maxLength int, format, content string) (string, bool) {
	truncatedMsg := fmt.Sprintf(format, len(content))
	if maxLength+len(truncatedMsg) < len(content) {
		return content[:maxLength] + truncatedMsg, true
	}
	return content, false
}

func (t *toolReductionMiddleware) BeforeModelRewriteState(ctx context.Context, state *adk.ChatModelAgentState) (context.Context, *adk.ChatModelAgentState, error) {
	if t.config.ToolOffload == nil {
		return ctx, state, nil
	}

	var (
		estimatedTokens int64
		idx2Token       = make([]int64, len(state.Messages))
		offloadConfig   = t.config.ToolOffload
	)

	// init msg tokens
	for i, msg := range state.Messages {
		tokens, err := offloadConfig.Tokenizer(msg)
		if err != nil {
			return ctx, state, err
		}
		estimatedTokens += tokens
		idx2Token[i] = tokens
	}

	if estimatedTokens < offloadConfig.ToolOffloadThreshold.MaxTokens {
		return ctx, state, nil
	}

	// calc range
	var (
		start = 0
		end   = len(state.Messages) - offloadConfig.ToolOffloadThreshold.RetentionSuffixLimit
	)
	for ; start < len(state.Messages); start++ {
		msg := state.Messages[start]
		if msg.Role == schema.Assistant && !getMsgOffloadedFlag(msg) {
			break
		}
	}
	if start >= end {
		return ctx, state, nil
	}

	// recursively handle
	tcMsgIndex := start
	batchCount := 0

	for tcMsgIndex < end {
		tcMsg := state.Messages[tcMsgIndex]
		if tcMsg.Role == schema.Assistant && len(tcMsg.ToolCalls) > 0 {
			j := tcMsgIndex
			for _, toolCall := range tcMsg.ToolCalls {
				j++
				if j >= end {
					break
				}
				resultMsg := state.Messages[j]
				if resultMsg.Role != schema.Tool { // unexpected
					break
				}
				tc, found := offloadConfig.ToolOffloadConfigMapping[toolCall.Function.Name]
				if !found {
					continue
				}

				offloadInfo, err := tc.OffloadHandler(ctx, &ToolDetail{
					Input: &compose.ToolInput{
						Name:      toolCall.Function.Name,
						Arguments: toolCall.Function.Arguments,
						CallID:    toolCall.ID,
					},
					Output: &compose.ToolOutput{
						Result: resultMsg.Content,
					},
				})
				if err != nil {
					return ctx, state, err
				}
				if !offloadInfo.NeedOffload {
					continue
				}

				writeErr := tc.OffloadBackend.Write(ctx, &filesystem.WriteRequest{
					FilePath: offloadInfo.FilePath,
					Content:  offloadInfo.OffloadContent,
				})
				if writeErr != nil {
					return ctx, state, writeErr
				}

				// calc tool msg tokens
				newTokens, err := offloadConfig.Tokenizer(resultMsg)
				if err != nil {
					return ctx, state, err
				}
				estimatedTokens -= idx2Token[j] - newTokens
			}

			// set dedup flag
			setMsgOffloadedFlag(tcMsg)

			// calc tool_call msg tokens
			newTokens, err := offloadConfig.Tokenizer(tcMsg)
			if err != nil {
				return ctx, state, err
			}
			estimatedTokens -= idx2Token[j] - newTokens
			batchCount++
		}

		if batchCount == offloadConfig.ToolOffloadThreshold.OffloadBatchSize {
			if estimatedTokens < offloadConfig.ToolOffloadThreshold.MaxTokens {
				break
			} else {
				batchCount = 0
			}
		}
	}

	return ctx, state, nil
}

// defaultTokenizer estimates tokens, which treats one token as ~4 characters of text for common English text.
// github.com/tiktoken-go/tokenizer is highly recommended to replace it.
func defaultTokenizer(msg *schema.Message) (int64, error) {
	if msg == nil {
		return 0, fmt.Errorf("message is nil")
	}
	if cached, ok := getMsgCachedToken(msg); ok {
		return cached, nil
	}

	var sb strings.Builder
	sb.WriteString(string(msg.Role))
	sb.WriteString("\n")
	sb.WriteString(msg.ReasoningContent)
	sb.WriteString("\n")
	sb.WriteString(msg.Content)
	sb.WriteString("\n")
	if msg.Role == schema.Assistant && len(msg.ToolCalls) > 0 {
		for _, tc := range msg.ToolCalls {
			sb.WriteString(tc.Function.Name)
			sb.WriteString("\n")
			sb.WriteString(tc.Function.Arguments)
		}
	}

	n := int64(len(sb.String()) / 4)
	setMsgCachedToken(msg, n)

	return n, nil
}

func defaultOffloadHandler(rootDir string) func(ctx context.Context, detail *ToolDetail) (*OffloadInfo, error) {
	return func(ctx context.Context, detail *ToolDetail) (*OffloadInfo, error) {
		fileName := detail.Input.CallID
		if fileName == "" {
			fileName = uuid.NewString()
		}
		filePath := filepath.Join(rootDir, fileName)
		nResult := fmt.Sprintf(toolOffloadResultFmt, filePath)
		offloadInfo := &OffloadInfo{
			NeedOffload:    true,
			FilePath:       filePath,
			OffloadContent: detail.Output.Result,
		}
		detail.Output.Result = nResult

		return offloadInfo, nil
	}
}

func getMsgOffloadedFlag(msg *schema.Message) (offloaded bool) {
	return msg.Extra != nil && msg.Extra[msgReducedFlag] != nil
}

func setMsgOffloadedFlag(msg *schema.Message) {
	if msg.Extra == nil {
		msg.Extra = map[string]interface{}{}
	}
	msg.Extra[msgReducedFlag] = struct{}{}
}

func getMsgCachedToken(msg *schema.Message) (int64, bool) {
	if msg.Extra == nil {
		return 0, false
	}
	tokens, ok := msg.Extra[msgReducedTokens].(int64)
	return tokens, ok
}

func setMsgCachedToken(msg *schema.Message, tokens int64) {
	if msg.Extra == nil {
		msg.Extra = map[string]interface{}{}
	}
	msg.Extra[msgReducedTokens] = tokens
}
