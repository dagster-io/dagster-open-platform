# Largely copied from python_modules/dagster/dagster_tests/components_tests/utils.py
# TODO: putting this here until we move these utilities into dagster.components.test


from typing import Any, TypeVar, Union

from dagster import Definitions
from dagster._utils.pydantic_yaml import enrich_validation_errors_with_source_position
from dagster.components import Component, ComponentLoadContext
from dagster.components.core.defs_module import CompositeYamlComponent, load_yaml_component
from dagster_shared import check
from dagster_shared.yaml_utils import parse_yaml_with_source_position
from pydantic import TypeAdapter

T = TypeVar("T")
T_Component = TypeVar("T_Component", bound=Component)


def load_context_and_component_for_test(
    component_type: type[T_Component], attrs: Union[str, dict[str, Any]]
) -> tuple[ComponentLoadContext, T_Component]:
    context = ComponentLoadContext.for_test()
    context = context.with_rendering_scope(component_type.get_additional_scope())
    model_cls = check.not_none(
        component_type.get_model_cls(), "Component must have schema for direct test"
    )
    if isinstance(attrs, str):
        source_positions = parse_yaml_with_source_position(attrs)
        with enrich_validation_errors_with_source_position(
            source_positions.source_position_tree, []
        ):
            attributes = TypeAdapter(model_cls).validate_python(source_positions.value)
    else:
        attributes = TypeAdapter(model_cls).validate_python(attrs)
    component = component_type.load(attributes, context)
    return context, component


def load_component_for_test(
    component_type: type[T_Component], attrs: Union[str, dict[str, Any]]
) -> T_Component:
    _, component = load_context_and_component_for_test(component_type, attrs)
    return component


def build_component_defs_for_test(
    component_type: type[Component], attrs: dict[str, Any]
) -> Definitions:
    context, component = load_context_and_component_for_test(component_type, attrs)
    return component.build_defs(context)


def build_component_and_defs(
    context: ComponentLoadContext,
    index: int = 0,
) -> tuple[Component, Definitions]:
    component = load_yaml_component(context)
    if isinstance(component, CompositeYamlComponent):
        comp_list = list(component.components)
        return comp_list[index], component.build_defs(context)
    else:
        return component, component.build_defs(context)
