
# Navigating the ASF Incubation Process

An overview of how to navigate the ASF incubating process from start to finish.

It can seem like a daunting task to bring a new open source project into the world. The Apache Software Foundation incubator can help you! Getting into the incubator and surviving incubation is straight forward when you know what to do and how to do it. This talk gives an overview of the ASF incubation process, the pitfalls to watch out for, and how projects become successful. It also gives an overview of the incubator PMC, how to interact with other podlings and make your mark on the ecosystem and finally how to make it as a top-level project at the Apache Software Foundation. It also goes over some the challenges that your project may face along the path to graduation.

## About the Apache Training (Incubating) Project

The purpose of the Training project is to create high-quality training material for various projects in an open source form. Up until now everyone who wants to offer a Training course for one of the Apache projects needs to create her or his own slides/labs and keep them up-to-date. This is a significant investment of time and money. This project aims to spread that burden and help all Apache projects as we can create shared resources and we can also create cross-project training resources.

## How to Get Involved

Vist [Apache Training (incubating)](https://training.apache.org) for more information on the project.
These slides can be found in [github](https://github.com/apache/incubator-training/tree/master/content/ApacheWay/NavigatingASFIncubation.). Pull requests welcome.

## Technology Used

The slides are generated from [asciidoctor](https://asciidoctor.org) markup and displayed with [reveal.js](https://asciidoctor.org/docs/asciidoctor-revealjs/). This means the content can be kept under version control and exported to a number of formats other than HTML.

## How to Use the Slides

First edit the author, email and position information found in the title slide ot the top of slide content file here:

`./src/main/asciidoc/index.adoc`

Editing any of the content there will change content on the slides.

Then edit the information that will appear in the about me slide found here:

`./src/main/asciidoc/aboutme.adoc`

Then build the slides to generate the HTML.

## How to Build

To install the needed dependencies on OSX run:

`install-deps.sh`

Then run:

`mvn clean compile`

## How to Update the ASF Statistics

To update the ASF statistics (no of podlings, no of committers etc. etc.) run:

`python3 stats.py > asciidoc/projectstats.adoc`

## How to View the Slides

Once built, the generated slides can be found at:

`target/generated-slides/index.html`

Just open the `index.html` in a browser to view the slides.

Some features require the slides to be viewed via a http/https url you can do this by running:

`mvn jetty:run-exploded`

And goto `http://127.0.0.1:8080/index.html` in a browser to view.

Some key shortcuts that may help you give a presentation:

- Cursor keys and space can navigate the slides.
- Press S will show speaker notes and a timer in a separate window.
- Press F for full screen.
- Press N for next slide or P for previous slide.
- Press O (for overview) will show a slide map / overview.
- Press B will black the screen.

## (ASF only) How to Make a Release

To remove the 3rd party dependencies in the release, clean, package and create the needed signature files, run this :

`release.sh <version>`

Or you can manually make a release in the usual ASF way.
