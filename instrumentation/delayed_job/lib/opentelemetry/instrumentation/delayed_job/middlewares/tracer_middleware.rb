# frozen_string_literal: true

# Copyright 2019 OpenTelemetry Authors
#
# SPDX-License-Identifier: Apache-2.0

require 'delayed/plugin'

module OpenTelemetry
  module Instrumentation
    module DelayedJob
      module Middlewares
        # Delayed Job plugin that instruments invoke_job and other hooks
        class TracerMiddleware < Delayed::Plugin

          class << self

            def instrument_invoke(job, &block)
              return block.call(job) unless enabled?

              tracer.in_span('delayed_job.invoke', kind: :consumer) do |span|
                span.set_attribute('delayed_job.id', job.id)
                span.set_attribute('delayed_job.name', job_name(job))
                span.set_attribute('delayed_job.queue', job.queue) if job.queue
                span.set_attribute('delayed_job.priority', job.priority)
                span.set_attribute('delayed_job.attempts', job.attempts)
                span.set_attribute('delayed_job.locked_by', job.locked_by)

                span.add_event('created_at', timestamp: job.created_at)
                span.add_event('run_at', timestamp: job.run_at) if job.run_at
                span.add_event('locked_at', timestamp: job.locked_at) if job.locked_at

                begin
                  yield job
                rescue StandardError => error
                  span.add_event('failed_at', timestamp: job.failed_at) if job.failed_at
                  raise error
                end
              end
            end

            def instrument_enqueue(job, &block)
              return block.call(job) unless enabled?

              tracer.in_span('delayed_job.enqueue', kind: :producer) do |span|
                yield job

                span.set_attribute('delayed_job.id', job.id)
                span.set_attribute('delayed_job.name', job_name(job))
                span.set_attribute('delayed_job.queue', job.queue) if job.queue
                span.set_attribute('delayed_job.priority', job.priority)
                span.add_event('created_at', timestamp: job.created_at)
                span.add_event('run_at', timestamp: job.run_at) if job.run_at
              end
            end

            # def flush(worker, &block)
            #   yield worker
            #
            #   tracer.shutdown if enabled?
            # end

            protected

            def enabled?
              DelayedJob::Instrumentation.instance.enabled?
            end

            def tracer
              DelayedJob::Instrumentation.instance.tracer
            end

            def job_name(job)
              # If Delayed Job is used via ActiveJob then get the job name from the payload
              return job.payload_object.job_data['job_class'] if job.payload_object.respond_to?(:job_data)

              job.name
            end
          end

          callbacks do |lifecycle|
            lifecycle.around(:invoke_job, &method(:instrument_invoke))
            lifecycle.around(:enqueue, &method(:instrument_enqueue))
            # lifecycle.around(:execute, &method(:flush))
          end
        end
      end
    end
  end
end
