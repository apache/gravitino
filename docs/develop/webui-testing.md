---
title: Web UI Testing
slug: /webui-test
license: "This software is licensed under the Apache License version 2."
---


## E2E test for WebUI

End-to-end testing for web frontends is conducted using the Java-based
[Selenium](https://www.selenium.dev/documentation/) testing framework.

Test cases can be found in the project directory:
`integration-test/src/test/java/org/apache/gravitino/integration/test/web/ui`,
where the `pages` directory is designated for storing definitions of frontend elements,
among others.
The root directory contains the actual steps for the test cases.

:::tip
While writing test cases, running them in a local environment may not pose any issues.

However, due to the limited performance capabilities of GitHub Actions,
scenarios involving delayed DOM loading -- such as the time taken
for a popup animation to open -- can result in test failures.

To circumvent this issue, it is necessary to manually insert a delay operation,
for instance, by adding `Thread.sleep(sleepTimeMillis)`.

This ensures that the test waits for the completion of the delay animation
before proceeding with the next operation, thereby avoiding the problem.

It is advisable to utilize the [`waits`](https://www.selenium.dev/documentation/webdriver/waits/)
methods inherent to Selenium as a substitute for `Thread.sleep()`.
:::

