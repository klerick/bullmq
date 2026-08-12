// Computes the next cron time with cron-parser (what BullMQ uses), so the Go
// port's cron scheduling can be cross-checked against Node.
// Usage: node cronnext.mjs <pattern> <tz> <afterMillis>
// Prints the next run time in ms epoch on stdout.
// BullMQ v6 calls CronExpressionParser.parse (cron-parser v5); the older
// parseExpression export is gone, so the harness must use the same entry point
// the library does.
import { CronExpressionParser } from 'cron-parser';

const [, , pattern, tz, afterMs] = process.argv;

const opts = { currentDate: new Date(parseInt(afterMs, 10)) };
if (tz) {
  opts.tz = tz;
}

const interval = CronExpressionParser.parse(pattern, opts);
process.stdout.write(String(interval.next().getTime()));
