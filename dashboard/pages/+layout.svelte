<script>
    import '@evidence-dev/tailwind/fonts.css';
    import '../app.css';
    import { EvidenceDefaultLayout } from '@evidence-dev/core-components';
    import { showQueries } from '@evidence-dev/component-utilities/stores';
    export let data;

    // Force-hide query result panels
    $showQueries = false;
</script>

<EvidenceDefaultLayout {data} neverShowQueries={true} title="Business Health">
    <slot slot="content" />
</EvidenceDefaultLayout>
