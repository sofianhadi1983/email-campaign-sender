# SES `MaxSendRate` utilization

## Scope

This note answers how the campaign sender should use `MaxSendRate`. Evidence is
limited to the AWS PDF snapshots in `examples/`.

## Findings

### Treat `MaxSendRate` as a sustained recipient rate, not an API-call rate

The SES v2 `SendQuota` type defines `MaxSendRate` as the maximum number of
emails the account can send per second in the current AWS Region. AWS also calls
it the maximum sending rate or TPS rate. The Developer Guide is more precise
about enforcement: sending quotas are based on recipients rather than messages;
an email with 10 recipients counts as 10. Therefore, the campaign's limiter
should debit one unit per recipient submitted, not one unit per
`SendBulkEmail` request.

`SendBulkEmail` accepts up to 50 destination objects per call, and each
destination may contain multiple `To`, `Cc`, or `Bcc` recipients. For this
one-person-per-personalization campaign, use one recipient in each destination,
so a full 50-entry request consumes 50 units of send-rate capacity.

Sources:

- `examples/ses-apiv2.pdf`, **SendQuota**, printed p. 533 (PDF p. 570).
- `examples/ses-dg.pdf`, **Managing your Amazon SES sending limits**, printed
  pp. 47–48 (PDF pp. 61–62).
- `examples/ses-dg.pdf`, **Using templates to send email**, printed
  pp. 117–118 (PDF pp. 131–132).

### Short bursts above the rate can succeed, but are not usable capacity

AWS says an account can exceed its sending rate for short bursts, but not for a
sustained period. It also warns that the actual rate at which SES accepts
messages can be lower than the account maximum. The sender should consequently
pace sustained traffic at or below its chosen target and treat any accepted
burst above that target as tolerance, not permission to run continuously above
`MaxSendRate`.

Source: `examples/ses-dg.pdf`, **Managing your Amazon SES sending limits**,
printed p. 47 (PDF p. 61).

### A 90% target is an engineering margin, not an AWS recommendation

The checked SES PDFs define the limit and describe burst behavior, but do not
recommend operating at 90% of `MaxSendRate`. A target such as
`0.90 * MaxSendRate` is therefore an application-selected margin for scheduling
jitter, concurrent requests, and the documented possibility that actual
acceptance is lower. It should not be presented as an AWS requirement.

For maximum reasonable speed, start with that margin, measure recipient
submissions over a rolling interval, and lower the target when SES reports
rate throttling. Gradual recovery toward the configured target after successful
traffic is an engineering policy; these PDF snapshots do not specify its
increase step or observation window.

### Throttling requires slowing down before retrying

AWS documents two relevant behaviors:

- When the API exceeds an account sending limit, SES returns a
  `ThrottlingException`; the Developer Guide says to wait for an interval of up
  to 10 minutes and then retry.
- The API v2 common-error definition says AWS SDKs automatically retry
  `ThrottlingException` and instructs callers to reduce request frequency.

Accordingly, explicit `Maximum sending rate exceeded` responses should feed
back into the limiter: reduce the sustained recipient rate, wait, and retry
subject to the campaign's separately chosen three-attempt policy. Do not rely
only on SDK retries, because the application still needs to control aggregate
concurrency and recipient throughput.

Sources:

- `examples/ses-dg.pdf`, **Errors related to the sending quotas for your Amazon
  SES account**, printed pp. 52–53 (PDF pp. 66–67).
- `examples/ses-dg.pdf`, **Amazon SES email sending errors**, printed p. 1230
  (PDF p. 1244).
- `examples/ses-apiv2.pdf`, **Common Error Types — ThrottlingException**,
  printed p. 582 (PDF p. 619).

## Uncertainties and boundaries

- The PDFs do not define the duration of a “short burst,” so the implementation
  must not derive a burst window or burst allowance from that statement.
- The PDFs do not prescribe 90%, an adaptive-decrease factor, a recovery slope,
  or jitter parameters.
- The “wait up to 10 minutes” API guidance does not prescribe one exact delay.
  The Developer Guide contains a progressively longer example for SMTP, but
  that SMTP-specific schedule is not evidence for an exact SES v2 API retry
  algorithm.
- AWS describes `MaxSendRate` as both emails per second and TPS. The Developer
  Guide's recipient-counting rule resolves the campaign-relevant unit: meter
  recipients, while also respecting `SendBulkEmail` request constraints.

## Recommendation for Question 9

Use one recipient per bulk destination, debit the limiter for every recipient,
and cap sustained submission at a configurable engineering target initially set
to 90% of the Region's current `MaxSendRate`. On explicit rate throttling,
reduce the target and apply the campaign's bounded retry policy; after sustained
success, recover gradually only up to the 90% target. Document 90% and all
adaptation constants as implementation choices rather than AWS guarantees.
