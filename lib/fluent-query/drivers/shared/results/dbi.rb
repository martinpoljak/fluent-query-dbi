# encoding: utf-8
require "fluent-query/drivers/result"
require "fluent-query/data"

module FluentQuery
    module Drivers
        module Shared
            module Results

                 ##
                 # PostgreSQL query native token.
                 #
                 
                 class DBI < FluentQuery::Drivers::Result

                    ##
                    # Brings resultset datasource.
                    # @var DBI::StatementHandle  result source
                    #

                    protected
                    @_source

                    ##
                    # Brings sources directory.
                    # @var Hash
                    #

                    protected
                    @@_sources = { }
                    
                    ##
                    # Initializes result.
                    #

                    public
                    def initialize(source)
                       super()
                       
                       @_source = source
                       @_columns = nil

                       ObjectSpace::define_finalizer(self, self.class.method(:finalize).to_proc)
                       @@_sources[self.object_id] = @_source
                    end

                    ##
                    # Dispatches object destroying.
                    #

                    public
                    def self.finalize(id)
                        if @@_sources[id]
                            @@_sources[id].finish
                            @@_sources.delete(id)
                        end
                    end

                    ##
                    # Returns all selected rows.
                    #

                    public
                    def all
                        self.to_enum.map { |i| i }
                    end

                    ##
                    # Returns one row.
                    #

                    public
                    def one
                        row = @_source.fetch_hash

                        if row
                            result = FluentQuery::Data[row]
                        else
                            result = nil
                        end

                        return result
                    end
                    
                    ##
                    # Returns first value of first row.
                    #

                    public
                    def single
                        result = @_source.fetch

                        if not result.nil?
                            result = result.first
                        else
                            result = nil
                        end

                        return result
                    end

                    ##
                    # Yields all rows as hashes.
                    #

                    public
                    def hash
                        result = nil
                        first = true
                        
                        while result or first
                            first = false
                            result = self.one

                            if result
                                yield result
                            end
                        end
                    end

                    ##
                    # Handles iterating.
                    #

                    public
                    def each(&block)
                        self.hash &block
                    end

                    ##
                    # Repeats the query leaded to the result.
                    #

                    public
                    def repeat!
                        @_source.execute
                        return self
                    end

                    ##
                    # Returns rows count.
                    #

                    public
                    def count
                        @_source.all.count
                    end

                    ##
                    # Frees result resources.
                    #

                    public
                    def free!
                        @_source.finish
                        @@_sources.delete(self.object_id)
                    end
                end
            end
        end
    end
end

