# vmug2023 Files


## Play with me LIVE DEMO: 1 to 100


```powershell

Measure-Command { 1..1000 | ForEach-Object -ThrottleLimit 500 -Parallel { sleep 1 ; $_ }  }

```

Try Tweeking the -ThrottleLimit integer and Sleep integer to see how it affects the total time
