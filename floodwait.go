package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

const maxFloodWait = 30 * time.Second
const floodWaitLimitExceeded = "FLOOD_WAIT_LIMIT_EXCEEDED"

func floodWaitMiddlewares() []telegram.Middleware {
	return []telegram.Middleware{telegram.MiddlewareFunc(func(next tg.Invoker) telegram.InvokeFunc {
		return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
			err := next.Invoke(ctx, input, output)
			if wait, ok := tgerr.AsFloodWait(err); ok && wait > maxFloodWait {
				return tgerr.New(420, fmt.Sprintf("%s_%d", floodWaitLimitExceeded, int(wait/time.Second)))
			}
			return err
		}
	})}
}
