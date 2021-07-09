#!/usr/bin/env bash

builddir=build
name=pg2sqlite
version=$(cat VERSION)

declare -a goos=(  darwin darwin linux linux windows windows freebsd freebsd )
declare -a goarch=(amd64  arm64  amd64 arm64 amd64   arm     amd64   arm     )

# Compress: os file
compress () {
  if [ "$1" == "windows" ]
  then
    zip -m "$2".zip "$2"
  else
    gzip "$2"
  fi
}

if [ ! -d $builddir ]
  then
    echo "Creating build directory"
    mkdir -p $builddir
  else
    echo "Cleaning build directory"
    rm $builddir/*
fi

echo "Building pg2sqlite ..."

for K in "${!goos[@]}"; do
  current_os=${goos[$K]}
  current_arch=${goarch[$K]}
  product="$name"_"$version"_$current_os-$current_arch
  if [ "$current_os" == "windows" ]; then product=$product.exe; fi
  echo Build "$K" : "$product"
  env GOOS="$current_os" GOARCH="$current_arch" go build -o $builddir/"$product"
  compress "$current_os" $builddir/"$product"
done

echo "Uploading to surge"

surge $builddir pg2sqlite.surge.sh