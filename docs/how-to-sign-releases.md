---
title: "How to Sign and Verify a Gravitino Releases"
date: 2023-11-01T014:00:00+11:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---
## How to Sign and Verify a Gravitino Releases

## Prerequisites

+ GPG/GnuPG
+ Release artifacts

## Platform Support

Instructions assume the commands are run on OSX, they may need modification to run on other platforms.

## Signing a release

1. If GPG or GnuPG is not installed, install it. This only needs to be done once.

    ```shell
    brew install gpg
    ```

2. Create a public/private key pair. This only needs to be done once. It recommended to set the expiry to 5 years and not enter a comment, all other defaults are fine.

    ```shell
    gpg --full-generate-key
    ```

Be sure to keep your private key secure. Do not forget your password and also record it securely somewhere.

3. To sign a release:

    ```shell
    gpg --detach-sign —armor <filename>.[zip|tar.gz]
    ```

    If you have more than one release file, sign each separately.

4. Generate hashes for a release:

    ```shell
    shasum -a 512 <filename>.[zip|tar.gz] > <filename>.[zip|tar.gz].sha512
    ```

5. Copy your public KEY to the KEYS file. This only needs to be done once.

    ```shell
    gpg --output KEY --armor --export <youremail>
    cat KEY >> KEYS
    ```

6. Publish hashes and signatures.

Upload the generated .sig and .sha512 files along with the release artifacts and KEYS files.

## Verifying a Release

1. Import the public KEYS used to sign releases.

    ```shell
    gpg --import KEYS
    ```

2. Verify the signature.

    ```shell
    gpg --verify <filename>.[zip|tar.gz].asc
    ```

    The output should contain the text "Good signature from ..."


3. Verify the hashes.

    ```shell
    diff -u <filename>.[zip|tar.gz]sha512 <(shasum -a 512 <filename>.[zip|tar.gz])
    ```

    The signatures should match and there be no differences.
