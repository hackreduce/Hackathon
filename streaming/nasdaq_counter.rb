#!/usr/bin/env ruby
 
STDIN.each_line do |line|
  word_count = {}
  fields = line.split(",")

  if fields[0] == "NASDAQ"
    puts "LongValueSum:#{fields[0]}\t1"
  end
end
