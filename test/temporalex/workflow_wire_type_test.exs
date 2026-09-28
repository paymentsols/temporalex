defmodule Temporalex.WorkflowWireTypeTest do
  @moduledoc """
  A workflow module's wire type must not depend on whether the module happens
  to be loaded. Outside a release, modules load lazily, and function_exported?/3
  is false for an unloaded module, so a fresh replay process, a client-only
  node or a parent starting a child on another worker used to fall back to the
  Elixir module name.
  """

  use ExUnit.Case, async: false

  @module Temporalex.WorkflowWireTypeTest.Unloaded

  setup do
    dir =
      Path.join(System.tmp_dir!(), "temporalex-wire-type-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    [{@module, beam}] =
      Code.compile_string("""
      defmodule #{inspect(@module)} do
        use Temporalex.Workflow, name: "Wire.Unloaded"
        def run(n), do: {:ok, n}
      end
      """)

    File.write!(Path.join(dir, "#{@module}.beam"), beam)
    :code.purge(@module)
    :code.delete(@module)
    :code.purge(@module)
    true = Code.prepend_path(dir)

    on_exit(fn ->
      Code.delete_path(dir)
      :code.purge(@module)
      :code.delete(@module)
      File.rm_rf!(dir)
    end)

    :ok
  end

  test "an unloaded module resolves to its declared wire type" do
    refute :code.is_loaded(@module)
    refute function_exported?(@module, :__workflow_type__, 0)
    assert Temporalex.Workflow.wire_type(@module) == "Wire.Unloaded"
  end

  test "a module that isn't a workflow falls back to its name" do
    assert Temporalex.Workflow.wire_type(Enum) == "Enum"
  end
end
