#!/bin/bash

echo -e "Building javadoc...\n"

./gradlew javadoc

echo -e "Publishing javadoc...\n"

cp -R build/docs/javadoc $HOME/javadoc

cd $HOME
git config --global user.email "travis@travis-ci.org"
git config --global user.name "travis-ci"
git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/RADAR-base/MongoDb-Sink-Connector.git gh-pages > /dev/null

cd gh-pages
git rm -rf ./javadoc
cp -Rf $HOME/javadoc .
git add -f .
git commit -m "Latest javadoc on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
git push -fq origin gh-pages > /dev/null

echo -e "Published Javadoc to gh-pages.\n"
